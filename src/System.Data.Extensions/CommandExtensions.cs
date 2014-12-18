using System.Collections.Generic;

namespace System.Data
{
    /// <summary>
    /// Extension methods for the IDbCommand interface
    /// </summary>
    public static class CommandExtensions
    {
        /// <summary>
        /// Adds a parameter to the command if the parametere name does not start with @ the method adds it internally
        /// </summary>
        /// <param name="cmd">The DbCommand that will be extended.</param>
        /// <param name="name">The parameter name.</param>
        /// <param name="value">The parameter value.</param>
        /// <param name="direction">The parameter direction.</param>
        /// <returns>Returns the added parameter</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", Justification = "Validated")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1", Justification = "Validated")]
        public static IDbDataParameter AddParameter(this IDbCommand cmd, string name, object value, ParameterDirection direction = ParameterDirection.Input)
        {
            return AddParameterInternal(cmd, name, value, null, null, direction);
        }

        /// <summary>
        /// Adds a parameter to the command if the parametere name does not start with @ the method adds it internally
        /// </summary>
        /// <param name="cmd">The DbCommand that will be extended.</param>
        /// <param name="name">The parameter name.</param>
        /// <param name="value">The parameter value.</param>
        /// <param name="dbType">The parameter type.</param>
        /// <param name="direction">The parameter direction.</param>
        /// <returns>Returns the added parameter</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", Justification = "Validated")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1", Justification = "Validated")]
        public static IDbDataParameter AddParameter(this IDbCommand cmd, string name, object value, DbType dbType, ParameterDirection direction = ParameterDirection.Input)
        {
            return AddParameterInternal(cmd, name, value, dbType, null, direction);
        }

        /// <summary>
        /// Adds a parameter to the command if the parametere name does not start with @ the method adds it internally
        /// </summary>
        /// <param name="cmd">The DbCommand that will be extended.</param>
        /// <param name="name">The parameter name.</param>
        /// <param name="value">The parameter value.</param>
        /// <param name="size">The parameter size.</param>
        /// <param name="direction">The parameter direction.</param>
        /// <returns>Returns the added parameter</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", Justification = "Validated")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1", Justification = "Validated")]
        public static IDbDataParameter AddParameter(this IDbCommand cmd, string name, object value, int size, ParameterDirection direction = ParameterDirection.Input)
        {
            return AddParameterInternal(cmd, name, value, null, size, direction);
        }

        /// <summary>
        /// Adds a parameter to the command if the parametere name does not start with @ the method adds it internally
        /// </summary>
        /// <param name="cmd">The DbCommand that will be extended.</param>
        /// <param name="name">The parameter name.</param>
        /// <param name="value">The parameter value.</param>
        /// <param name="dbType">The parameter type.</param>
        /// <param name="size">The parameter size.</param>
        /// <param name="direction">The parameter direction.</param>
        /// <returns>Returns the added parameter</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", Justification = "Validated")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1", Justification = "Validated")]
        public static IDbDataParameter AddParameter(this IDbCommand cmd, string name, object value, DbType dbType, int size, ParameterDirection direction = ParameterDirection.Input)
        {
            return AddParameterInternal(cmd, name, value, dbType, size, direction);
        }

        /// <summary>
        /// Adds an input / output parameter.
        /// </summary>
        /// <param name="cmd">The DbCommand that will be extended.</param>
        /// <param name="name">The parameter name.</param>
        /// <param name="value">The parameter value.</param>
        /// <param name="dbType">The parameter type.</param>
        /// <param name="size">The parameter size.</param>
        /// <returns>
        /// Returns the added parameter
        /// </returns>
        public static IDbDataParameter AddInOutParameter(this IDbCommand cmd, string name, object value, DbType? dbType = null, int? size = null)
        {
            return AddParameterInternal(cmd, name, value, dbType, size, ParameterDirection.InputOutput);
        }

        /// <summary>
        /// Adds an output parameter.
        /// </summary>
        /// <param name="cmd">The DbCommand that will be extended.</param>
        /// <param name="name">The parameter name.</param>
        /// <param name="dbType">The parameter type.</param>
        /// <param name="size">The parameter size.</param>
        /// <returns>Returns the added parameter</returns>
        public static IDbDataParameter AddOutParameter(this IDbCommand cmd, string name, DbType? dbType = null, int? size = null)
        {
            return AddParameterInternal(cmd, name, null, dbType, size, ParameterDirection.Output);
        }

        /// <summary>
        /// Adds a return parameter.
        /// </summary>
        /// <param name="cmd">The DbCommand that will be extended.</param>
        /// <param name="name">The parameter name.</param>
        /// <param name="dbType">The parameter type.</param>
        /// <param name="size">The parameter size.</param>
        /// <returns>Returns the added parameter</returns>
        public static IDbDataParameter AddReturnParameter(this IDbCommand cmd, string name, DbType? dbType = null, int? size = null)
        {
            return AddParameterInternal(cmd, name, null, dbType, size, ParameterDirection.ReturnValue);
        }

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
        public static IList<T> ExecuteReader<T>(this IDbCommand cmd, Func<IDataRecord, T> convert, CommandBehavior cmdBehavior = CommandBehavior.Default)
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

            using (var reader = cmd.ExecuteReader(cmdBehavior))
            {
                while (reader != null && reader.Read())
                {
                    output.Add(convert(reader));
                }
            }

            return output;
        }

        private static IDbDataParameter AddParameterInternal(this IDbCommand cmd, string name, object value, DbType? dbType, int? size, ParameterDirection direction)
        {
            if (cmd == null)
            {
                throw new ArgumentNullException("cmd", "The cmd cannot be null");
            }

            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentOutOfRangeException("name", "The name cannot be null or whitespace");
            }

            var param = cmd.CreateParameter();
            param.ParameterName = name.StartsWith("@", StringComparison.OrdinalIgnoreCase) ? name : "@" + name;
            param.Value = value;
            param.Direction = direction;
            if (dbType.HasValue) param.DbType = dbType.Value;
            if (size.HasValue) param.Size = size.Value;
            cmd.Parameters.Add(param);
            return param;
        }
    }
}
