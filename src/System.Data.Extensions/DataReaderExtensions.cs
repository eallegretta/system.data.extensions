namespace System.Data
{
    /// <summary>
    /// Extension methods for the IDataReader interface
    /// </summary>
    public static class DataReaderExtensions
    {
        /// <summary>
        /// Gets the specified field from the data reader and converts it to the destination value
        /// </summary>
        /// <typeparam name="T">The type to convert to</typeparam>
        /// <param name="reader">The reader.</param>
        /// <param name="index">The index.</param>
        /// <returns>The casted value</returns>
        public static T Value<T>(this IDataReader reader, int index)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("reader", "The reader cannot be null");
            }

            if (index < 0)
            {
                throw new ArgumentOutOfRangeException("index", "The index cannot be less than zero");
            }

            var type = typeof(T);

            if (type.IsEnum)
            {
                return reader.IsDBNull(index) ? default(T) : (T)Enum.ToObject(type, reader.GetValue(index));
            }

            if (reader.IsDBNull(index))
            {
                return default(T);
            }

            return (T)Convert.ChangeType(reader.GetValue(index), typeof(T));
        }

        /// <summary>
        /// Gets the specified field from the data reader and converts it to the destination value
        /// </summary>
        /// <typeparam name="T">The type to convert to</typeparam>
        /// <param name="reader">The reader.</param>
        /// <param name="name">The field name.</param>
        /// <returns>The casted value</returns>
        public static T Value<T>(this IDataReader reader, string name)
        {
            return Value<T>(reader, reader.GetOrdinal(name));
        }
    }
}
