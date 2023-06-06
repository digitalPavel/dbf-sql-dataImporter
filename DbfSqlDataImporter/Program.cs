using ParserDbf;
using BIISqlLib;
using System.Data;
using Microsoft.Data.SqlClient;

namespace DbfToSqlDataImporter;

public class Program
{
    public static void Main(string[] args)
    {
        try
        {
            DataTable datatable = ConvertTo.Table(args[0]);// args[0] is dbf file
            Console.WriteLine("Convertation to table executed successfully.");
            ExecuteStoredProcForClipperRecord(datatable);
            Console.WriteLine("Stored procedure executed successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("An error occurred: " + ex.Message);
        }
    }

    public static void ExecuteStoredProcForClipperRecord(DataTable dataTable)
    {
        // Connect to SQL using the Test connection type
        using (SqlCommand command = SqlServer.GetCommand(SqlServer.ConnectionTypes.Test))
        {
            // Set the stored procedure name
            command.CommandText = "clipper_bandaidcarton";

            int rowNumber = 0;

            foreach (DataRow record in dataTable.Rows)
            {
                rowNumber++;

                if (string.IsNullOrWhiteSpace(record["deleted"].ToString()))
                {
                    try
                    {
                        // Extract the values from the current row
                        string cartonNum = record["ref"].ToString().Trim(); // Replace "ref" with the actual column name
                        string trackmom = record["TRACK_MOM"].ToString().Trim();
                        string trackno = record["TRACK_NO"].ToString().Trim();
                        decimal actualWeight = decimal.Parse(record["ACT_WGT"].ToString().Trim());
                        string shippingMethod = record["CARRIER"].ToString().Trim();
                        decimal freightCost = decimal.Parse(record["TOT_CHG"].ToString().Trim());

                        // Clear previous parameters
                        command.Parameters.Clear();

                        // Add parameters to the command
                        command.Parameters.Add("@cartonnum", SqlDbType.VarChar).Value = cartonNum;
                        command.Parameters.Add("@tracknumex", SqlDbType.VarChar).Value = trackmom;
                        command.Parameters.Add("@tracknum", SqlDbType.VarChar).Value = trackno;
                        command.Parameters.Add("@shipdate", SqlDbType.DateTime).Value = DBNull.Value;
                        command.Parameters.Add("@actualweight", SqlDbType.Decimal).Value = actualWeight;
                        command.Parameters.Add("@freightCost", SqlDbType.Decimal).Value = freightCost;
                        command.Parameters.Add("@shippingMethod", SqlDbType.Char).Value = shippingMethod;

                        // Execute the stored procedure
                        command.ExecuteNonQuery();

                    }
                    catch (Exception ex)
                    {
                        // Log exceptions to output
                        Console.WriteLine("An error occurred for row: " + rowNumber + ". Error message: " + ex.Message);
                    }

                }
            }
        }
    }
}




