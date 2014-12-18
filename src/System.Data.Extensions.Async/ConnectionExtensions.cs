using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace System.Data
{
    /// <summary>
    /// Extension methods for the IDbConnection interface
    /// </summary>
    public static class ConnectionExtensions
    {
        /// <summary>
        /// Executes a database command and returns a data reader.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="storedProcedureName">Name of the stored procedure.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="disposeConnection">if set to <c>true</c> [dispose connection].</param>
        /// <returns>
        /// The data reader
        /// </returns>
        public static Task<IDataReader> ExecuteReaderAsync(this IDbConnection connection, string storedProcedureName, object commandParameters = null, int? commandTimeout = null, bool disposeConnection = false)
        {
            return ExecuteReaderAsync(connection, storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout, disposeConnection);
        }

        /// <summary>
        /// Executes a database command and returns a data reader.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="commandText">The command text.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="disposeConnection">if set to <c>true</c> [closes the connection].</param>
        /// <returns>
        /// The data reader
        /// </returns>
        public static Task<IDataReader> ExecuteReaderAsync(
            this IDbConnection connection,
            string commandText,
            CommandType commandType = CommandType.StoredProcedure,
            object commandParameters = null,
            int? commandTimeout = null,
            bool disposeConnection = false)
        {
            EnsureConnection(connection);

            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            Func<Task<IDataReader>> executeReader = async () =>
                {
                    using (var cmd = connection.CreateCommand(commandText, commandType, commandParameters, commandTimeout))
                    {
                        var dbCmd = cmd as DbCommand;

                        if (dbCmd != null)
                        {
                            var dataReader = await dbCmd.ExecuteReaderAsync();
                            return dataReader as IDataReader;
                        }

                        return cmd.ExecuteReader();
                    }
                };

            if (disposeConnection)
            {
                using (connection)
                {
                    return executeReader();
                }
            }
            
            return executeReader();
        }

        

        /// <summary>
        /// Executes a data reader and applies the convert function on each record so it return an IList of T.
        /// </summary>
        /// <typeparam name="T">The type of the record to return</typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="convert">The convert function for each record.</param>
        /// <param name="storedProcedureName">Name of the stored procedure.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="commandBehavior">The command behavior.</param>
        /// <param name="disposeConnection">if set to <c>true</c> [dispose connection].</param>
        /// <returns>
        /// An IList of T.
        /// </returns>
        public static Task<IList<T>> ExecuteReaderAsync<T>(
            this IDbConnection connection,
            Func<IDataRecord, T> convert,
            string storedProcedureName,
            object commandParameters = null,
            int? commandTimeout = null,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            bool disposeConnection = false)
        {
            return ExecuteReaderAsync(connection, convert, storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout, commandBehavior, disposeConnection);
        }

        /// <summary>
        /// Executes a data reader and applies the convert function on each record so it return an IList of T.
        /// </summary>
        /// <typeparam name="T">The type of the record to return</typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="convert">The convert function for each record.</param>
        /// <param name="commandText">The command text.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="commandBehavior">The command behavior.</param>
        /// <param name="disposeConnection">if set to <c>true</c> [dispose connection].</param>
        /// <returns>
        /// An IList of T.
        /// </returns>
        public static Task<IList<T>> ExecuteReaderAsync<T>(
            this IDbConnection connection,
            Func<IDataRecord, T> convert,
            string commandText,
            CommandType commandType = CommandType.StoredProcedure,
            object commandParameters = null,
            int? commandTimeout = null,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            bool disposeConnection = false)
        {
            EnsureConnection(connection);

            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }
            
            Func<Task<IList<T>>> executeReader = () => connection.CreateCommand(commandText, commandType, commandParameters, commandTimeout).ExecuteReaderAsync(convert, commandBehavior);

            if (disposeConnection)
            {
                using (connection)
                {
                    return executeReader();
                }
            }

            return executeReader();
        }

        /// <summary>
        /// Executes a database command and returns the value of the first column of the first row.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="storedProcedureName">Name of the stored procedure.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="disposeConnection">if set to <c>true</c> [dispose connection].</param>
        /// <returns>
        /// The value
        /// </returns>
        public static Task<object> ExecuteScalarAsync(this IDbConnection connection, string storedProcedureName, object commandParameters = null, int? commandTimeout = null, bool disposeConnection = false)
        {
            return ExecuteScalarAsync(connection, storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout, disposeConnection);
        }

        /// <summary>
        /// Executes a database command and returns the value of the first column of the first row.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="commandText">The command text.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="disposeConnection">if set to <c>true</c> [close connection].</param>
        /// <returns>
        /// The value
        /// </returns>
        public static Task<object> ExecuteScalarAsync(
            this IDbConnection connection,
            string commandText,
            CommandType commandType = CommandType.StoredProcedure,
            object commandParameters = null,
            int? commandTimeout = null,
            bool disposeConnection = false)
        {
            EnsureConnection(connection);

            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            Func<Task<object>> executeScalar = () =>
            {
                using (var cmd = connection.CreateCommand(commandText, commandType, commandParameters, commandTimeout))
                {
                    var dbCmd = cmd as DbCommand;

                    if (dbCmd != null)
                    {
                        return dbCmd.ExecuteScalarAsync();
                    }

                    return Task.FromResult(cmd.ExecuteScalar());
                }
            };

            if (disposeConnection)
            {
                using (connection)
                {
                    return executeScalar();
                }
            }

            return executeScalar();
        }

        /// <summary>
        /// Executes a database query and returns the number of affected rows.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="storedProcedureName">Name of the stored procedure.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="disposeConnection">if set to <c>true</c> [dispose connection].</param>
        /// <returns>
        /// The number of affected rows
        /// </returns>
        public static Task<int> ExecuteNonQueryAsync(this IDbConnection connection, string storedProcedureName, object commandParameters = null, int? commandTimeout = null, bool disposeConnection = false)
        {
            return ExecuteNonQueryAsync(connection, storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout, disposeConnection);
        }

        /// <summary>
        /// Executes a database query and returns the number of affected rows.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="commandText">The commmand text.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="disposeConnection">if set to <c>true</c> [close connection].</param>
        /// <returns>
        /// The number of affected rows
        /// </returns>
        public static Task<int> ExecuteNonQueryAsync(
            this IDbConnection connection,
            string commandText,
            CommandType commandType = CommandType.StoredProcedure,
            object commandParameters = null,
            int? commandTimeout = null,
            bool disposeConnection = false)
        {
            EnsureConnection(connection);

            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            Func<Task<int>> executeNonQuery = () =>
            {
                using (var cmd = connection.CreateCommand(commandText, commandType, commandParameters, commandTimeout))
                {
                    var dbCmd = cmd as DbCommand;

                    if (dbCmd != null)
                    {
                        return dbCmd.ExecuteNonQueryAsync();
                    }

                    return Task.FromResult(cmd.ExecuteNonQuery());
                }
            };

            if (disposeConnection)
            {
                using (connection)
                {
                    return executeNonQuery();
                }
            }

            return executeNonQuery();
        }

        private static void EnsureConnection(IDbConnection connection)
        {
            if (connection == null)
            {
                throw new ArgumentNullException("connection", "The connection cannot be null");
            }
        }
    }
}
