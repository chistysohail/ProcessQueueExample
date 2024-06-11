using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace ProcessUserReport
{
    class Program
    {
        private static string connectionString = "YourConnectionStringHere";

        static async Task Main(string[] args)
        {
            await ProcessReportsAsync();
        }

        private static async Task ProcessReportsAsync()
        {
            var newReports = await GetNewReportsAsync();

            foreach (var report in newReports)
            {
                try
                {
                    await UpdateReportStatusAsync(report.Id, 1); // Set status to Processing
                    // Simulate processing the report
                    await Task.Delay(1000);
                    await CompleteReportProcessingAsync(report.Id);
                }
                catch (Exception ex)
                {
                    await FailReportProcessingAsync(report.Id, ex);
                }
            }
        }

        private static async Task<List<UserReport>> GetNewReportsAsync()
        {
            var reports = new List<UserReport>();

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand("SELECT * FROM UserReport WHERE Status = 0", connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var report = new UserReport
                            {
                                Id = reader.GetInt32(reader.GetOrdinal("Id")),
                                StartProcessDate = reader.IsDBNull(reader.GetOrdinal("StartProcessDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StartProcessDate")),
                                EndProcessDate = reader.IsDBNull(reader.GetOrdinal("EndProcessDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EndProcessDate")),
                                StartDate = reader.IsDBNull(reader.GetOrdinal("StartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StartDate")),
                                EndDate = reader.IsDBNull(reader.GetOrdinal("EndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EndDate")),
                                Status = reader.GetInt32(reader.GetOrdinal("Status")),
                                IsError = reader.GetBoolean(reader.GetOrdinal("IsError")),                                
                            };

                            reports.Add(report);
                        }
                    }
                }
            }

            return reports;
        }

        private static async Task UpdateReportStatusAsync(int id, int status)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand("UPDATE UserReport SET Status = @Status, StartProcessDate = @StartProcessDate WHERE Id = @Id", connection))
                {
                    command.Parameters.AddWithValue("@Status", status);
                    command.Parameters.AddWithValue("@StartProcessDate", DateTime.Now);
                    command.Parameters.AddWithValue("@Id", id);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private static async Task CompleteReportProcessingAsync(int id)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand("UPDATE UserReport SET Status = @Status, EndProcessDate = @EndProcessDate WHERE Id = @Id", connection))
                {
                    command.Parameters.AddWithValue("@Status", 2); // Completed
                    command.Parameters.AddWithValue("@EndProcessDate", DateTime.Now);
                    command.Parameters.AddWithValue("@Id", id);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private static async Task FailReportProcessingAsync(int id, Exception ex)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand("UPDATE UserReport SET Status = @Status, EndProcessDate = @EndProcessDate, IsError = @IsError WHERE Id = @Id", connection))
                {
                    command.Parameters.AddWithValue("@Status", 3); // Failed
                    command.Parameters.AddWithValue("@EndProcessDate", DateTime.Now);
                    command.Parameters.AddWithValue("@IsError", true);
                    command.Parameters.AddWithValue("@Id", id);

                    await command.ExecuteNonQueryAsync();
                }
            }

            // Log the error
            Console.WriteLine($"Error processing report with Id {id}: {ex.Message}");
        }
    }

    public class UserReport
    {
        public int Id { get; set; }
        public DateTime? StartProcessDate { get; set; }
        public DateTime? EndProcessDate { get; set; }
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public int Status { get; set; }
        public bool IsError { get; set; }
       
    }
}
