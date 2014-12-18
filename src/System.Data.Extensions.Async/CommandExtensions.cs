using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace System.Data
{
    /// <summary>
    /// Extension methods for the IDbCommand interface
    /// </summary>
    public static class CommandExtensions
    {
        /// <summary>
        /// Executes a data reader and applies the convert function on each record so it return an IList of T.
        /// </summary>
        /// <typeparam name="T">The type of the record to return</typeparam>
        /// <param name="cmd">The command to extend.</param>
        /// <param name="convert">The convert function for each record.</param>
        /// <param name="cmdBehavior">The command behavior.</param>
        /// <returns>An IList of T.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", Justification = "Validated")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1", Justification = "Validated")]
        public static async Task<IList<T>> ExecuteReaderAsync<T>(this IDbCommand cmd, Func<IDataRecord, T> convert, CommandBehavior cmdBehavior = CommandBehavior.Default)
        {
            if (cmd == null)
            {
                throw new ArgumentNullException("cmd", "The cmd cannot be null");
            }

            if (convert == null)
            {
                throw new ArgumentNullException("convert", "The convert function cannot be null");
            }

            var output = new List<T>();

            var dbCmd = cmd as DbCommand;

            if (dbCmd == null)
            {
                using (var reader = cmd.ExecuteReader(cmdBehavior))
                {
                    while (reader != null && reader.Read())
                    {
                        output.Add(convert(reader));
                    }
                }
            }
            else
            {
                using (var reader = await dbCmd.ExecuteReaderAsync(cmdBehavior))
                {
                    while (reader != null && await reader.ReadAsync())
                    {
                        output.Add(convert(reader));
                    }
                }  
            }

            return output;    
        }
    }
}
