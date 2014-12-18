using System.Data;
using System.Data.Common;

namespace Cinchcast.Framework.Data
{
    /// <summary>
    /// Defines the contract for classes that will create instances of the <see cref="System.Data.IDataAdapter"/> interface.
    /// </summary>
    public interface IDataAdapterFactory
    {
        /// <summary>
        /// Creates a new instance of the data adapter.
        /// </summary>
        /// <returns>A new instance of <see cref="System.Data.IDataAdapter"/></returns>
        DbDataAdapter CreateDataAdapter();
    }
}
