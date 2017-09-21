using System.Reflection;
using Microsoft.WindowsAzure.Storage.Table;

namespace Orleans.Azure
{
    /// <summary>
    ///  Interface for storage entity property converter.  Converts properties to/from storage properties
    ///  Must convert complext types to either string or binary data.
    /// </summary>
    public interface IPropertyConverter
    {
        /// <summary>
        /// Attempt to read a user property from storage property.
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="userProperty"></param>
        /// <param name="storageProperty"></param>
        /// <returns></returns>
        void ConvertFromStorage(object obj, PropertyInfo userProperty, EntityProperty storageProperty);

        /// <summary>
        /// Attempt to write a user property to storage property.
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="userProperty"></param>
        /// <param name="storageProperty"></param>
        /// <returns></returns>
        void ConvertToStorage(object obj, PropertyInfo userProperty, out EntityProperty storageProperty);
    }
}
