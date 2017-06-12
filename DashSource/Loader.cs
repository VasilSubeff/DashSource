using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace DashSource
{
    public class Loader
    {
        public static List<string> getTableColumns(string tableName)
        {
            List<string> listOfColumns = new List<string>();
            using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.ConnectionString))
            {
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "select c.name from sys.columns c inner join sys.tables t on t.object_id = c.object_id and t.name = '" + tableName + "' and t.type = 'U'";
                    connection.Open();
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            listOfColumns.Add(reader.GetString(0));
                        }
                    }
                }
            }

            return listOfColumns;
        }

        public static DataTable GetDataTabletFromCSVFile(string sourceFilePath)
        {
            DataTable pipeData = new DataTable();
            try
            {
                using (TextFieldParser pipeReader = new TextFieldParser(sourceFilePath))
                {
                    pipeReader.SetDelimiters(new string[] { Properties.Settings.Default.Delimiter });
                    pipeReader.HasFieldsEnclosedInQuotes = true;

                    string[] colFields = pipeReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        pipeData.Columns.Add(datecolumn);
                    }
                    while (!pipeReader.EndOfData)
                    {
                        string[] fieldData = pipeReader.ReadFields();
                        //Making empty value as null.
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        pipeData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                return null;
            }
            return pipeData;
        }

        public static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable fileData, string tableName, List<string> columnNames)
        {
            using (SqlConnection dbConnection = new SqlConnection(Properties.Settings.Default.ConnectionString))
            {
                dbConnection.Open();
                using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                {
                    s.DestinationTableName = tableName;
                    var columns = fileData.Columns;

                    for (int i = 0; i < columns.Count; i++)
                    {
                        s.ColumnMappings.Add(columns[i].ToString(), columnNames[i].ToString());
                    }
                    s.WriteToServer(fileData);
                }
            }
        }
    }
}
