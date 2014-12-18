using Cinchcast.Framework.Data;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Reflection;

namespace System.Data
{
    /// <summary>
    /// Extension methods for the IDbConnection interface
    /// </summary>
    public static class ConnectionExtensions
    {
        /// <summary>
        /// Creates a stored procedure command.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="storedProcedureName">Name of the stored procedure.</param>
        /// <returns>
        /// A database command
        /// </returns>
        public static IDbCommand CreateCommand(this IDbConnection connection, string storedProcedureName)
        {
            return connection.CreateCommand(storedProcedureName, CommandType.StoredProcedure, null, null);
        }

        /// <summary>
        /// Creates a stored procedure command.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="storedProcedureName">Name of the stored procedure.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <returns>
        /// A database command
        /// </returns>
        public static IDbCommand CreateCommand(this IDbConnection connection, string storedProcedureName, object commandParameters = null, int? commandTimeout = null)
        {
            return connection.CreateCommand(storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout);
        }

        /// <summary>
        /// Creates a command.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="commandText">The command text.</param>
        /// <param name="commandType">The command type.</param>
        /// <param name="commandParameters">The command parameters.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <returns>
        /// A database command
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", Justification = "Validated")]
        public static IDbCommand CreateCommand(
            this IDbConnection connection,
            string commandText,
            CommandType commandType = CommandType.StoredProcedure,
            object commandParameters = null,
            int? commandTimeout = null)
        {
            EnsureConnection(connection);

            if (string.IsNullOrWhiteSpace(commandText))
            {
                throw new ArgumentNullException("commandText", "The commandText cannot be null or whitespace");
            }

            var command = connection.CreateCommand();
            command.CommandType = commandType;
            command.CommandText = commandText;
            if (commandTimeout.HasValue)
            {
                command.CommandTimeout = commandTimeout.Value;
            }

            if (commandParameters != null)
            {
                var parameters = commandParameters is IDictionary<string, object> ? (IDictionary<string, object>)commandParameters : commandParameters.ToDictionary();

                foreach (var commandParameter in parameters)
                {
                    command.AddParameter(commandParameter.Key, commandParameter.Value);
                }
            }

            return command;
        }

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
        public static IDataReader ExecuteReader(this IDbConnection connection, string storedProcedureName, object commandParameters = null, int? commandTimeout = null, bool disposeConnection = false)
        {
            return ExecuteReader(connection, storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout, disposeConnection);
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
        public static IDataReader ExecuteReader(
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

            Func<IDataReader> executeReader = () =>
                {
                    using (var cmd = connection.CreateCommand(commandText, commandType, commandParameters, commandTimeout))
                    {
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
        public static IList<T> ExecuteReader<T>(
            this IDbConnection connection,
            Func<IDataRecord, T> convert,
            string storedProcedureName,
            object commandParameters = null,
            int? commandTimeout = null,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            bool disposeConnection = false)
        {
            return ExecuteReader(connection, convert, storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout, commandBehavior, disposeConnection);
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
        public static IList<T> ExecuteReader<T>(
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
            
            Func<IList<T>> executeReader = () => connection.CreateCommand(commandText, commandType, commandParameters, commandTimeout).ExecuteReader(convert, commandBehavior);

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
        public static object ExecuteScalar(this IDbConnection connection, string storedProcedureName, object commandParameters = null, int? commandTimeout = null, bool disposeConnection = false)
        {
            return ExecuteScalar(connection, storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout, disposeConnection);
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
        public static object ExecuteScalar(
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

            Func<object> executeScalar = () =>
            {
                using (var cmd = connection.CreateCommand(commandText, commandType, commandParameters, commandTimeout))
                {
                    return cmd.ExecuteScalar();
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
        public static int ExecuteNonQuery(this IDbConnection connection, string storedProcedureName, object commandParameters = null, int? commandTimeout = null, bool disposeConnection = false)
        {
            return ExecuteNonQuery(connection, storedProcedureName, CommandType.StoredProcedure, commandParameters, commandTimeout, disposeConnection);
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
        public static int ExecuteNonQuery(
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

            Func<int> executeNonQuery = () =>
            {
                using (var cmd = connection.CreateCommand(commandText, commandType, commandParameters, commandTimeout))
                {
                    return cmd.ExecuteNonQuery();
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

        /// <summary>
        /// Creates the data adapter.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="selectCommand">The select command.</param>
        /// <param name="insertCommand">The insert command.</param>
        /// <param name="updateCommand">The update command.</param>
        /// <param name="deleteCommand">The delete command.</param>
        /// <returns>
        /// A new instance of the IDataAdapter
        /// </returns>
        public static DbDataAdapter CreateDataAdapter(
            this IDbConnection connection,
            IDbCommand selectCommand = null,
            IDbCommand insertCommand = null,
            IDbCommand updateCommand = null,
            IDbCommand deleteCommand = null)
        {
            EnsureConnection(connection);

            DbDataAdapter adapter = null;

            // Quick lookup for defined types to avoid reflection
            if (connection is SqlConnection) adapter = new SqlDataAdapter();
            if (connection is OleDbConnection) adapter = new OleDbDataAdapter();
            if (connection is OdbcConnection) adapter = new OdbcDataAdapter();
            if (connection is IDataAdapterFactory) adapter = ((IDataAdapterFactory)connection).CreateDataAdapter();

            if (adapter == null)
            {
                adapter = GetAdapterUsingReflection(connection, connection);
            }

            if (adapter == null) throw new Exception("Could not create IDataAdapter for " + connection.GetType());

            adapter.SelectCommand = selectCommand as DbCommand;
            adapter.InsertCommand = insertCommand as DbCommand;
            adapter.UpdateCommand = updateCommand as DbCommand;
            adapter.DeleteCommand = deleteCommand as DbCommand;

            return adapter;
        }

        private static DbDataAdapter GetAdapterUsingReflection(IDbConnection connection, IDbConnection dbConnection)
        {
            var dbProviderFactory = dbConnection.GetType().GetProperty("DbProviderFactory", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(dbConnection, null) as DbProviderFactory;

            if (dbProviderFactory == null)
            {
                // Look for the ConnectionFactory property    
                var dbConnectionFactoryProperty = dbConnection.GetType().GetProperty("ConnectionFactory", BindingFlags.Instance | BindingFlags.NonPublic);

                if (dbConnectionFactoryProperty == null) throw new Exception("Could not determine the DbProviderFactory for " + connection.GetType());

                var connectionFactory = dbConnectionFactoryProperty.GetValue(dbConnection, null);

                dbProviderFactory = connectionFactory.GetType().GetProperty("ProviderFactory", BindingFlags.Instance | BindingFlags.Public).GetValue(connectionFactory, null) as DbProviderFactory;

                if (dbProviderFactory == null) throw new Exception("Could not determine the DbProviderFactory for " + connection.GetType());
            }

            return dbProviderFactory.CreateDataAdapter();
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
