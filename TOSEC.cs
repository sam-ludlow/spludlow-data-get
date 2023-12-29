using System.Data;
using System.IO.Compression;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Web;
using System.Xml;
using System.Xml.Linq;
using HtmlAgilityPack;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace spludlow_data_get
{
	
	public class TOSEC
	{
		private readonly Dictionary<string, string> parameters;
		private readonly HttpClient httpClient;
		private readonly string rootDirectory;

		public TOSEC(Dictionary<string, string> _parameters, HttpClient _httpClient)
		{
			parameters = _parameters;
			httpClient = _httpClient;

			rootDirectory = Path.Join(parameters["DIRECTORY"], "TOSEC");

			Directory.CreateDirectory(rootDirectory);
		}

		public async Task<int> Get()
		{
			Tools.RequiredParamters(parameters, "GET", ["DIRECTORY", "ACTION"]);

			string baseUrl = "https://www.tosecdev.org/downloads";

			string html = await httpClient.GetStringAsync(baseUrl);

			HtmlDocument doc = new();
			doc.LoadHtml(html);

			SortedDictionary<string, string> versionLinks = [];

			foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//div[@class='pd-subcategory']"))
			{
				HtmlNode linkNode = node.ChildNodes.Single();

				string text = linkNode.InnerText;

				if (text.Length != 10)
					continue;

				string href = linkNode.Attributes["href"].Value;

				href = new Uri(new Uri(baseUrl), href).AbsoluteUri;

				versionLinks.Add(text, href);
			}

			string version = versionLinks.Keys.Last();
			baseUrl = versionLinks[version];

			string versionDirectory = Path.Combine(rootDirectory, version);

			if (Directory.Exists(versionDirectory) == true)
				return 0;

			Directory.CreateDirectory(versionDirectory);

			html = await httpClient.GetStringAsync(baseUrl);
			doc = new();
			doc.LoadHtml(html);

			string? foundUrl = null;

			foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//a"))
			{
				string? href = node.Attributes["href"]?.Value;

				if (href == null)
					continue;

				if (node.InnerText.ToLower().EndsWith(".zip") == false)
					continue;

				if (foundUrl == null)
					foundUrl = new Uri(new Uri(baseUrl), href).AbsoluteUri;
				else
					throw new ApplicationException("Found more than one ZIP link.");
			}

			if (foundUrl == null)
				throw new ApplicationException("Did not find ZIP link.");

			string archiveFilename = Path.Combine(versionDirectory, "TOSEC.zip");

			Console.Write($"NEW downloading {version} {foundUrl} => {archiveFilename} ...");

			using (Stream sourceStream = await httpClient.GetStreamAsync(foundUrl))
			{
				using FileStream targetSStream = new(archiveFilename, FileMode.Create);
				sourceStream.CopyTo(targetSStream);
			}

			Console.WriteLine("...done");

			Console.Write($"extracting archive {archiveFilename} ...");
			
			ZipFile.ExtractToDirectory(archiveFilename, versionDirectory);
			
			Console.WriteLine("...done");

			return 1;
		}

		public string LocalVersion()
		{
			List<string> versions = new ();

			foreach (string directory in Directory.GetDirectories(rootDirectory))
			{
				string version = Path.GetFileName(directory);
				if (version.Length == 10)
					versions.Add(version);
			}

			if (versions.Count == 0)
				throw new ApplicationException("No local version directories");

			versions.Sort();

			return versions[^1];
		}

		public async Task<int> Load()
		{
			Tools.RequiredParamters(parameters, "LOAD", ["VERSION", "MSSQL_SERVER", "MSSQL_TARGET_NAME"]);

			string version = parameters["VERSION"];
			if (version == "0")
				version = LocalVersion();
	
			DataSet dataSet = new DataSet();

			DataTable reportTable = Tools.MakeDataTable(
				"Category	Filename	FileVersion",
				"String		String		String"
			);

			string versionDirectory = Path.Combine(rootDirectory, version);

			foreach (string category in new string[] { "TOSEC", "TOSEC-ISO", "TOSEC-PIX" })
			{
				string categoryDirectory = Path.Combine(versionDirectory, category);

				foreach (string filename in Directory.GetFiles(categoryDirectory, "*.dat"))
				{
					string name = Path.GetFileNameWithoutExtension(filename);

					int index;

					index = name.LastIndexOf("(");
					if (index == -2)
						throw new ApplicationException("No last index of open bracket");

					string fileVersion = name.Substring(index).Trim(new char[] { '(', ')' });
					name = name.Substring(0, index).Trim();

					DataSet fileDataSet = Tools.ImportXML(filename);

					TrimStrings(fileDataSet);
					MoveHeader(fileDataSet, category, Path.GetFileName(filename));

					foreach (DataTable table in fileDataSet.Tables)
						foreach (DataColumn column in table.Columns)
							column.AutoIncrement = false;

					DataRow reportRow = reportTable.Rows.Add(category, filename, fileVersion);

					DataTable datafileTable = fileDataSet.Tables["datafile"];
					if (datafileTable == null || datafileTable.Rows.Count != 1)
						throw new ApplicationException("No header table or row: " + filename);
					DataRow datafileRow = datafileTable.Rows[0];
					foreach (DataColumn column in datafileTable.Columns)
					{
						if (column.ColumnName.EndsWith("_Id") == true)
							continue;
						if (reportTable.Columns.Contains(column.ColumnName) == false)
							reportTable.Columns.Add(column.ColumnName, column.DataType);

						reportRow[column.ColumnName] = datafileRow[column.ColumnName];
					}

					MergeDataSet(fileDataSet, dataSet);
				}
			}

			//Tools.PopText(reportTable);

			Tools.DataSet2MSSQL(dataSet, parameters["MSSQL_SERVER"], parameters["MSSQL_TARGET_NAME"]);

			return 0;
		}

		private static XmlReaderSettings _XmlReaderSettings = new XmlReaderSettings()
		{
			DtdProcessing = DtdProcessing.Parse,
			IgnoreComments = false,
			IgnoreWhitespace = true,
		};

		public async Task<int> Payload()
		{
			Tools.RequiredParamters(parameters, "LOAD", ["VERSION", "MSSQL_SERVER", "MSSQL_TARGET_NAME"]);

			string version = parameters["VERSION"];
			if (version == "0")
				version = LocalVersion();

			string versionDirectory = Path.Combine(rootDirectory, version);

			SqlConnection connection = new SqlConnection($"{parameters["MSSQL_SERVER"]}Initial Catalog='{parameters["MSSQL_TARGET_NAME"]}';");

			DataTable datafileTable = Tools.ExecuteFill(connection, "SELECT * FROM [datafile]");
			DataTable gameTable = Tools.ExecuteFill(connection, "SELECT * FROM [game]");

			DataTable datafilePayloadTable = new DataTable($"datafile_payload");
			datafilePayloadTable.Columns.Add("datafile_key", typeof(string));
			datafilePayloadTable.Columns.Add("title", typeof(string));
			datafilePayloadTable.Columns.Add("xml", typeof(string));
			datafilePayloadTable.Columns.Add("json", typeof(string));
			datafilePayloadTable.Columns.Add("html", typeof(string));

			DataTable gamePayloadTable = new DataTable($"game_payload");
			gamePayloadTable.Columns.Add("datafile_key", typeof(string));
			gamePayloadTable.Columns.Add("game_key", typeof(string));
			gamePayloadTable.Columns.Add("title", typeof(string));
			gamePayloadTable.Columns.Add("xml", typeof(string));
			gamePayloadTable.Columns.Add("json", typeof(string));
			gamePayloadTable.Columns.Add("html", typeof(string));
			gamePayloadTable.PrimaryKey = [gamePayloadTable.Columns["datafile_key"], gamePayloadTable.Columns["game_key"]];

			int datafile_key_length = 0;
			int game_key_length = 0;

			foreach (DataRow datafileRow in datafileTable.Rows)
			{
				long datafile_id = (long)datafileRow["datafile_id"];
				string datafile_description = (string)datafileRow["description"];

				string datafile_key = Uri.EscapeDataString(datafile_description);
				datafile_key_length = Math.Max(datafile_key_length, datafile_key.Length);

				string category = (string)datafileRow["category"];
				string filename = (string)datafileRow["filename"];

				string datFilename = Path.Combine(versionDirectory, category, filename);

				if (File.Exists(datFilename) == false)
					throw new ApplicationException($"DAT filename missing: {datFilename}");

				XDocument xDocument = XDocument.Load(datFilename);

				string title = "";
				string xml = xDocument.ToString();
				string json = XML2JSON(xDocument.Root);
				string html = "";

				datafilePayloadTable.Rows.Add(datafile_key, title, xml, json, html);

				using (XmlReader reader = XmlReader.Create(datFilename, _XmlReaderSettings))
				{
					reader.MoveToContent();

					while (reader.Read())
					{
						while (reader.NodeType == XmlNodeType.Element && reader.Name == "game")
						{
							XElement element = XElement.ReadFrom(reader) as XElement;
							if (element != null)
							{
								string game_description = element.Element("description").Value;

								string game_key = Uri.EscapeDataString(game_description);
								game_key_length = Math.Max(game_key_length, game_key.Length);

								title = "";
								xml = element.ToString();
								json = XML2JSON(element);
								html = "";

								if (gamePayloadTable.Rows.Find(new object[] { datafile_key, game_key }) != null)
								{
									Console.WriteLine($"DUPLICATE {category} {datafile_description} {game_description}");
								}
								else
								{
									gamePayloadTable.Rows.Add(datafile_key, game_key, title, xml, json, html);
								}
							}
						}
					}
				}
			}

			MakeMSSQLPayloadsInsert(datafilePayloadTable, parameters["MSSQL_SERVER"], parameters["MSSQL_TARGET_NAME"],
				["datafile_key"],
				[$"VARCHAR({datafile_key_length})"]);

			MakeMSSQLPayloadsInsert(gamePayloadTable, parameters["MSSQL_SERVER"], parameters["MSSQL_TARGET_NAME"],
				["datafile_key", "game_key"],
				[$"VARCHAR({datafile_key_length})", $"VARCHAR({game_key_length})"]);

			return 0;
		}

		public static string XML2JSON(XElement element)
		{
			JsonSerializerSettings serializerSettings = new JsonSerializerSettings();
			serializerSettings.Formatting = Newtonsoft.Json.Formatting.Indented;

			using (StringWriter writer = new StringWriter())
			{
				CustomJsonWriter customJsonWriter = new CustomJsonWriter(writer);

                Newtonsoft.Json.JsonSerializer jsonSerializer = Newtonsoft.Json.JsonSerializer.Create(serializerSettings);
				jsonSerializer.Serialize(customJsonWriter, element);

				return writer.ToString();
			}
		}

		public static void XML2JSON(string inputXmlFilename, string outputJsonFilename)
		{
			XmlDocument xmlDocument = new XmlDocument();
			xmlDocument.Load(inputXmlFilename);

			JsonSerializerSettings serializerSettings = new JsonSerializerSettings();
			serializerSettings.Formatting = Newtonsoft.Json.Formatting.Indented;

			using (StreamWriter streamWriter = new StreamWriter(outputJsonFilename, false, new UTF8Encoding(false)))
			{
				CustomJsonWriter customJsonWriter = new CustomJsonWriter(streamWriter);

				Newtonsoft.Json.JsonSerializer jsonSerializer = Newtonsoft.Json.JsonSerializer.Create(serializerSettings);
				jsonSerializer.Serialize(customJsonWriter, xmlDocument);
			}
		}

		public async Task<int> Payloadhtml()
		{
			Tools.RequiredParamters(parameters, "LOAD", ["VERSION", "MSSQL_SERVER", "MSSQL_TARGET_NAME"]);

			SqlConnection connection = new SqlConnection($"{parameters["MSSQL_SERVER"]}Initial Catalog='{parameters["MSSQL_TARGET_NAME"]}';");

			DataTable datafileTable = Tools.ExecuteFill(connection, "SELECT * FROM [datafile]");
			DataTable gameTable = Tools.ExecuteFill(connection, "SELECT * FROM [game]");
			DataTable romTable = Tools.ExecuteFill(connection, "SELECT * FROM [rom]");

			StringBuilder tosecHtml = new StringBuilder();

			SqlCommand datafileCommand = new SqlCommand("UPDATE datafile_payload SET [title] = @title, [html] = @html WHERE [datafile_key] = @datafile_key", connection);
			datafileCommand.Parameters.Add("@title", SqlDbType.NVarChar);
			datafileCommand.Parameters.Add("@html", SqlDbType.NVarChar);
			datafileCommand.Parameters.Add("@datafile_key", SqlDbType.VarChar);

			SqlCommand gameCommand = new SqlCommand("UPDATE game_payload SET [title] = @title, [html] = @html WHERE ([datafile_key] = @datafile_key AND [game_key] = @game_key)", connection);
			gameCommand.Parameters.Add("@title", SqlDbType.NVarChar);
			gameCommand.Parameters.Add("@html", SqlDbType.NVarChar);
			gameCommand.Parameters.Add("@datafile_key", SqlDbType.VarChar);
			gameCommand.Parameters.Add("@game_key", SqlDbType.VarChar);

			connection.Open();

			try
			{
				foreach (string category in new string[] { "TOSEC", "TOSEC-ISO", "TOSEC-PIX" })
				{
					if (tosecHtml.Length > 0)
						tosecHtml.AppendLine("<br />");

					tosecHtml.AppendLine($"<h2>{category}</h2>");

					DataRow[] datafileCategoryRows = datafileTable.Select($"category = '{category}'");

					foreach (DataRow datafileRow in datafileCategoryRows)
					{
						string datafile_description = (string)datafileRow["description"];

						//if (datafile_description.Contains("BBC") == false)
						//	continue;

						Console.WriteLine(datafile_description);

						string datafile_key = Uri.EscapeDataString(datafile_description);

						string datafile_link = $"/tosec/{datafile_key}";

						datafileRow["description"] = $"<a href=\"{datafile_link}\">{datafile_description}</a>";

						//
						// datafile
						//

						StringBuilder dataFileHtml = new StringBuilder();

						long datafile_id = (long)datafileRow["datafile_id"];

						DataRow[] gameRows = gameTable.Select($"datafile_id = {datafile_id}");

						foreach (DataRow gameRow in gameRows)
						{
							long game_id = (long)gameRow["game_id"];

							string game_description = (string)gameRow["description"];

							string game_key = Uri.EscapeDataString(game_description);

							string game_link = $"/tosec/{datafile_key}/{game_key}";

							gameRow["description"] = $"<a href=\"{game_link}\">{game_description}</a>";

							//
							// game
							//

							StringBuilder gameHtml = new StringBuilder();

							gameHtml.AppendLine("<h2>datafile</h2>");
							gameHtml.AppendLine(Tools.MakeHtmlTable(datafileTable, [datafileRow], null));

							gameHtml.AppendLine("<hr />");

							gameHtml.AppendLine("<h2>game</h2>");
							gameHtml.AppendLine(Tools.MakeHtmlTable(gameTable, [gameRow], null));

							gameHtml.AppendLine("<hr />");

							DataRow[] romRows = romTable.Select($"game_id = {game_id}");
							gameHtml.AppendLine("<h2>rom</h2>");
							gameHtml.AppendLine(Tools.MakeHtmlTable(romTable, romRows, null));

							gameCommand.Parameters["@title"].Value = $"{game_description} / {datafile_description} - TOSEC game";
							gameCommand.Parameters["@html"].Value = gameHtml.ToString();
							gameCommand.Parameters["@datafile_key"].Value = datafile_key;
							gameCommand.Parameters["@game_key"].Value = game_key;

							gameCommand.ExecuteNonQuery();

						}

						dataFileHtml.AppendLine(Tools.MakeHtmlTable(gameTable, gameRows, null));

						datafileCommand.Parameters["@title"].Value = $"{datafile_description} - TOSEC datafile";
						datafileCommand.Parameters["@html"].Value = dataFileHtml.ToString();
						datafileCommand.Parameters["@datafile_key"].Value = datafile_key;

						datafileCommand.ExecuteNonQuery();
					}

					tosecHtml.AppendLine(Tools.MakeHtmlTable(datafileTable, datafileCategoryRows, null));
				}
			}
			finally
			{
				connection.Close();
			}

			DataTable table = new DataTable($"tosec_payload");
			table.Columns.Add("tosec_payload_id", typeof(string));
			table.Columns.Add("title", typeof(string));
			table.Columns.Add("xml", typeof(string));
			table.Columns.Add("json", typeof(string));
			table.Columns.Add("html", typeof(string));

			table.Rows.Add("1", "", "", "", tosecHtml.ToString());

			MakeMSSQLPayloadsInsert(table, parameters["MSSQL_SERVER"], parameters["MSSQL_TARGET_NAME"],
				["tosec_payload_id"],
				["VARCHAR(1)"]);


			return 0;
		}

		public static void MakeMSSQLPayloadsInsert(DataTable table, string serverConnectionString, string databaseName, string[] primaryKeyNames, string[] primaryKeyTypes)
		{
			using (SqlConnection targetConnection = new SqlConnection(serverConnectionString + $"Initial Catalog='{databaseName}';"))
			{
				List<string> columnDefs = new List<string>();

				for (int index = 0; index < primaryKeyNames.Length; ++index)
					columnDefs.Add($"{primaryKeyNames[index]} {primaryKeyTypes[index]}");

				columnDefs.Add("[title] NVARCHAR(MAX)");
				columnDefs.Add("[xml] NVARCHAR(MAX)");
				columnDefs.Add("[json] NVARCHAR(MAX)");
				columnDefs.Add("[html] NVARCHAR(MAX)");

				columnDefs.Add($"CONSTRAINT [PK_{table.TableName}] PRIMARY KEY NONCLUSTERED ({String.Join(", ", primaryKeyNames)})");

				string commandText = $"CREATE TABLE [{table.TableName}] ({String.Join(", ", columnDefs)});";

				Console.WriteLine(commandText);
				Tools.ExecuteNonQuery(targetConnection, commandText);

				Tools.BulkInsert(targetConnection, table);
			}
		}

		public static void MoveHeader(DataSet dataSet, string category, string filename)
		{
			DataTable headerTable = dataSet.Tables["header"];
			DataTable datafileTable = dataSet.Tables["datafile"];

			if (headerTable == null || headerTable.Rows.Count != 1)
				throw new ApplicationException("Did not find one headerTable row");

			if (datafileTable == null || datafileTable.Rows.Count != 1)
				throw new ApplicationException("Did not find one datafileTable row");

			headerTable.Rows[0]["category"] = category;
			
			headerTable.Columns.Add("filename", typeof(string));
			headerTable.Rows[0]["filename"] = filename;

			foreach (DataColumn column in headerTable.Columns)
			{
				if (column.ColumnName.EndsWith("_id") == true)
					continue;

				if (datafileTable.Columns.Contains(column.ColumnName) == false)
					datafileTable.Columns.Add(column.ColumnName, typeof(string));

				datafileTable.Rows[0][column.ColumnName] = headerTable.Rows[0][column.ColumnName];
			}

			dataSet.Tables.Remove("header");
		}

		public static void TrimStrings(DataSet dataSet)
		{
			Type type = typeof(string);

			foreach (DataTable table in dataSet.Tables)
			{
				foreach (DataColumn column in table.Columns)
				{
					if (column.DataType != type)
						continue;

					int ordinal = column.Ordinal;

					foreach (DataRow row in table.Rows)
					{
						if (row.IsNull(ordinal) == true)
							continue;

						string value = ((string)row[ordinal]).Trim();
						if (value.Length == 0)
							row[ordinal] = DBNull.Value;
						else
							row[ordinal] = value;
					}
				}
			}
		}


		public static void MergeDataSet(DataSet sourceDataSet, DataSet targetDataSet)
		{
			foreach (DataTable sourceTable in sourceDataSet.Tables)
			{
				sourceTable.PrimaryKey = new DataColumn[0];

				DataTable targetTable = null;
				if (targetDataSet.Tables.Contains(sourceTable.TableName) == false)
				{
					targetTable = new DataTable(sourceTable.TableName);
					targetDataSet.Tables.Add(targetTable);
				}
				else
				{
					targetTable = targetDataSet.Tables[sourceTable.TableName];
				}

				foreach (DataColumn column in sourceTable.Columns)
				{
					column.Unique = false;

					if (targetTable.Columns.Contains(column.ColumnName) == false)
					{
						DataColumn targetColumn = targetTable.Columns.Add(column.ColumnName, column.DataType);
						targetColumn.Unique = false;
					}

				}
			}

			Dictionary<string, long> addIds = new Dictionary<string, long>();
			foreach (DataTable sourceTable in sourceDataSet.Tables)
			{
				if (targetDataSet.Tables.Contains(sourceTable.TableName) == true)
					addIds.Add(sourceTable.TableName + "_id", targetDataSet.Tables[sourceTable.TableName].Rows.Count);
				else
					throw new ApplicationException($"Table not in target: {sourceTable.TableName}");
			}


			foreach (DataTable sourceTable in sourceDataSet.Tables)
			{
				foreach (DataColumn column in sourceTable.Columns)
				{
					if (column.ColumnName.EndsWith("_id") == false)
						continue;

					foreach (DataRow row in sourceTable.Rows)
						row[column] = (long)row[column] + addIds[column.ColumnName];
				}

				DataTable targetTable = targetDataSet.Tables[sourceTable.TableName];

				foreach (DataRow row in sourceTable.Rows)
					targetTable.ImportRow(row);
			}
		}


		//public async Task<int> Xml()
		//{
		//	Tools.RequiredParamters(parameters, "XML", ["DIRECTORY", "ACTION"]);


		//	//Console.WriteLine("Hello " + userAgent);

		//	return 0;
		//}

	}
}
