using System.Collections.Generic;

namespace System
{
    internal static class ObjectExtensions
    {
        /// <summary>
        /// Creates a dictionary bases on object properties.
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns>A dictionary with object properties</returns>
        internal static IDictionary<string, TValue> ToDictionary<TValue>(this object source)
        {
            if (source == null) return null;

            IDictionary<string, TValue> dict = new Dictionary<string, TValue>();

            foreach (var property in source.GetType().GetProperties())
            {
                dict.Add(property.Name, (TValue)property.GetValue(source, null));
            }

            return dict;
        }

        /// <summary>
        /// Creates a dictionary bases on object properties.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <returns>A dictionary with object properties</returns>
        internal static IDictionary<string, object> ToDictionary(this object source)
        {
            return ToDictionary<object>(source);
        }
    }
}
