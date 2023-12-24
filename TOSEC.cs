using System.Data;
using System.IO.Compression;

using HtmlAgilityPack;

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
					MoveHeader(fileDataSet);

					foreach (DataTable table in fileDataSet.Tables)
						foreach (DataColumn column in table.Columns)
							column.AutoIncrement = false;

					DataRow reportRow = reportTable.Rows.Add(category, name, fileVersion);

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

		public static void MoveHeader(DataSet dataSet)
		{
			DataTable headerTable = dataSet.Tables["header"];
			DataTable datafileTable = dataSet.Tables["datafile"];

			if (headerTable == null || headerTable.Rows.Count != 1)
				throw new ApplicationException("Did not find one headerTable row");

			if (datafileTable == null || datafileTable.Rows.Count != 1)
				throw new ApplicationException("Did not find one datafileTable row");

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
