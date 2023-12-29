using System.Data;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Xml.Linq;

using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace spludlow_data_get
{
	public class CustomJsonWriter : JsonTextWriter
	{
		public CustomJsonWriter(TextWriter writer) : base(writer) { }
		public override void WritePropertyName(string name)
		{
			if (name.StartsWith("@") == true)
				base.WritePropertyName(name.Substring(1));
			else
				base.WritePropertyName(name);
		}
	}
	public class Tools
	{
		public static void RequiredParamters(Dictionary<string, string> parameters, string action, string[] requiredNames)
		{
			foreach (string name in requiredNames)
				if (parameters.ContainsKey(name) == false)
					throw new ApplicationException($"The action \"{action}\" requires the paramters \"{string.Join(", ", requiredNames)}\".");
		}


		public static DataSet ImportXML(string filename)
		{
			XElement document = XElement.Load(filename);

			DataSet dataSet = new DataSet();

			ImportXML(document, dataSet, null);

			return dataSet;
		}

		public static void ImportXML(XElement element, DataSet dataSet, DataRow? parentRow)
		{
			string tableName = element.Name.LocalName;

			string? forignKeyName = null;
			if (parentRow != null)
				forignKeyName = parentRow.Table.TableName + "_id";

			DataTable table;

			if (dataSet.Tables.Contains(tableName) == false)
			{
				table = new DataTable(tableName);
				DataColumn pkColumn = table.Columns.Add(tableName + "_id", typeof(long));
				pkColumn.AutoIncrement = true;
				pkColumn.AutoIncrementSeed = 1;

				table.PrimaryKey = new DataColumn[] { pkColumn };

				if (parentRow != null)
					table.Columns.Add(forignKeyName, parentRow.Table.Columns[forignKeyName].DataType);

				dataSet.Tables.Add(table);
			}
			else
			{
				table = dataSet.Tables[tableName];
			}

			Dictionary<string, string> rowValues = new Dictionary<string, string>();

			foreach (XAttribute attribute in element.Attributes())
				rowValues.Add(attribute.Name.LocalName, attribute.Value);

			foreach (XElement childElement in element.Elements())
			{
				if (childElement.HasAttributes == false && childElement.HasElements == false)
					rowValues.Add(childElement.Name.LocalName, childElement.Value);
			}

			foreach (string columnName in rowValues.Keys)
			{
				if (table.Columns.Contains(columnName) == false)
					table.Columns.Add(columnName, typeof(string));
			}

			DataRow row = table.NewRow();

			if (parentRow != null)
				row[forignKeyName] = parentRow[forignKeyName];

			foreach (string columnName in rowValues.Keys)
				row[columnName] = rowValues[columnName];

			table.Rows.Add(row);

			foreach (XElement childElement in element.Elements())
			{
				if (childElement.HasAttributes == true || childElement.HasElements == true)
					ImportXML(childElement, dataSet, row);
			}
		}

		public static void DataSet2MSSQL(DataSet dataSet, string serverConnectionString, string databaseName)
		{
			SqlConnection targetConnection = new SqlConnection(serverConnectionString);

			if (DatabaseExists(targetConnection, databaseName) == true)
				return;

			ExecuteNonQuery(targetConnection, $"CREATE DATABASE[{databaseName}]");

			targetConnection = new SqlConnection($"{serverConnectionString}Initial Catalog='{databaseName}';");

			foreach (DataTable table in dataSet.Tables)
			{
				List<string> columnDefs = new List<string>();

				foreach (DataColumn column in table.Columns)
				{
					int max = 1;
					if (column.DataType.Name == "String")
					{
						foreach (DataRow row in table.Rows)
						{
							if (row.IsNull(column) == false)
								max = Math.Max(max, ((string)row[column]).Length);
						}
					}

					switch (column.DataType.Name)
					{
						case "String":
							columnDefs.Add($"[{column.ColumnName}] NVARCHAR({max})");
							break;

						case "Int64":
							columnDefs.Add($"[{column.ColumnName}] BIGINT" + (columnDefs.Count == 0 ? " NOT NULL" : ""));
							break;

						default:
							throw new ApplicationException($"SQL Bulk Copy, Unknown datatype {column.DataType.Name}");
					}
				}

				columnDefs.Add($"CONSTRAINT [PK_{table.TableName}] PRIMARY KEY NONCLUSTERED ([{table.Columns[0].ColumnName}])");

				string createText = $"CREATE TABLE [{table.TableName}]({String.Join(", ", columnDefs.ToArray())});";

				Console.WriteLine(createText);
				ExecuteNonQuery(targetConnection, createText);

				BulkInsert(targetConnection, table);
			}
		}

		public static void PopText(DataTable table)
		{
			PopText(TextTable(table));
		}
		public static void PopText(string text)
		{
			string filename = Path.GetTempFileName();
			File.WriteAllText(filename, text, Encoding.UTF8);
			Process.Start("notepad.exe", filename);
			Environment.Exit(0);
		}

		public static string TextTable(DataTable table)
		{
			StringBuilder result = new StringBuilder();

			foreach (DataColumn column in table.Columns)
			{
				if (column.Ordinal != 0)
					result.Append('\t');

				result.Append(column.ColumnName);
			}
			result.AppendLine();

			foreach (DataColumn column in table.Columns)
			{
				if (column.Ordinal != 0)
					result.Append('\t');

				result.Append(column.DataType);
			}
			result.AppendLine();

			foreach (DataRow row in table.Rows)
			{
				foreach (DataColumn column in table.Columns)
				{
					if (column.Ordinal != 0)
						result.Append('\t');

					object value = row[column];

					if (value != null)
						result.Append(Convert.ToString(value));
				}
				result.AppendLine();
			}

			return result.ToString();
		}

		public static DataTable MakeDataTable(string columnNames, string columnTypes)
		{
			string[] names = columnNames.Split(new char[] { '\t' }, StringSplitOptions.RemoveEmptyEntries);
			string[] types = columnTypes.Split(new char[] { '\t' }, StringSplitOptions.RemoveEmptyEntries);

			if (names.Length != types.Length)
				throw new ApplicationException("Make Data Table Bad definition.");

			DataTable table = new DataTable();

			List<int> keyColumnIndexes = new List<int>();

			for (int index = 0; index < names.Length; ++index)
			{
				string name = names[index];
				string typeName = "System." + types[index];

				if (typeName.EndsWith("*") == true)
				{
					typeName = typeName.Substring(0, typeName.Length - 1);
					keyColumnIndexes.Add(index);
				}

				table.Columns.Add(name, Type.GetType(typeName, true));
			}

			if (keyColumnIndexes.Count > 0)
			{
				List<DataColumn> keyColumns = new List<DataColumn>();
				foreach (int index in keyColumnIndexes)
					keyColumns.Add(table.Columns[index]);
				table.PrimaryKey = keyColumns.ToArray();
			}

			return table;
		}

		public static string MakeHtmlTable(DataTable table, string? tableStyle)
		{
			return MakeHtmlTable(table, table.Rows.OfType<DataRow>(), tableStyle);
		}

		public static string MakeHtmlTable(DataTable table, IEnumerable<DataRow> rows, string? tableStyle)
		{
			StringBuilder html = new StringBuilder();

			html.Append("<table");
			if (tableStyle != null)
			{
				html.Append(" style=\"");
				html.Append(tableStyle);
				html.Append("\"");
			}
			html.AppendLine(">");

			html.Append("<tr>");
			foreach (DataColumn column in table.Columns)
			{
				if (column.ColumnName.EndsWith("_id") == true)
					continue;

				html.Append("<th>");
				html.Append(column.ColumnName);
				html.Append("</th>");
			}
			html.AppendLine("</tr>");

			foreach (DataRow row in rows)
			{
				html.Append("<tr>");
				foreach (DataColumn column in table.Columns)
				{
					if (column.ColumnName.EndsWith("_id") == true)
						continue;

					html.Append("<td>");
					if (row.IsNull(column) == false)
					{
						string value = Convert.ToString(row[column]);

						if (value.StartsWith("<a href=") == true)
							html.Append(value);
						else
							html.Append(WebUtility.HtmlEncode(value));
					}
					html.Append("</td>");
				}
				html.AppendLine("</tr>");
			}

			html.AppendLine("</table>");

			return html.ToString();
		}


		//
		// MS SQL
		//

		public static bool DatabaseExists(SqlConnection connection, string databaseName)
		{
			object obj = ExecuteScalar(connection, $"SELECT name FROM sys.databases WHERE name = '{databaseName}'");

			if (obj == null || obj is DBNull)
				return false;

			return true;
		}

		public static object ExecuteScalar(SqlConnection connection, string commandText)
		{
			connection.Open();
			try
			{
				using (SqlCommand command = new SqlCommand(commandText, connection))
					return command.ExecuteScalar();
			}
			finally
			{
				connection.Close();
			}

		}

		public static int ExecuteNonQuery(SqlConnection connection, string commandText)
		{
			connection.Open();
			try
			{
				using (SqlCommand command = new SqlCommand(commandText, connection))
					return command.ExecuteNonQuery();
			}
			finally
			{
				connection.Close();
			}
		}
		public static DataTable ExecuteFill(SqlConnection connection, string commandText)
		{
			DataSet dataSet = new DataSet();
			using (SqlDataAdapter adapter = new SqlDataAdapter(commandText, connection))
				adapter.Fill(dataSet);
			return dataSet.Tables[0];
		}

		public static void BulkInsert(SqlConnection connection, DataTable table)
		{
			using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connection))
			{
				sqlBulkCopy.DestinationTableName = table.TableName;

				sqlBulkCopy.BulkCopyTimeout = 15 * 60;

				connection.Open();
				try
				{
					sqlBulkCopy.WriteToServer(table);
				}
				finally
				{
					connection.Close();
				}
			}
		}

		public static string[] TableList(SqlConnection connection)
		{
			List<string> result = new List<string>();

			DataTable table = ExecuteFill(connection,
				"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME");

			foreach (DataRow row in table.Rows)
				result.Add((string)row["TABLE_NAME"]);

			return result.ToArray();
		}


	}


}
