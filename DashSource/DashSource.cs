using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DashSource
{
    class DashSource
    {
        static void Main(string[] args)
        {
            string sourceFilePath = @"C:\Users\vasil\Desktop\Dimo\KPI_Status_12-Jun-2017.csv";
            string tableName = "dr_kpis";
            
            var columnNames = Loader.getTableColumns(tableName);
            var parsedFile = Loader.GetDataTabletFromCSVFile(sourceFilePath);
            Loader.InsertDataIntoSQLServerUsingSQLBulkCopy(parsedFile, tableName, columnNames);           
        }
        
    }
}
