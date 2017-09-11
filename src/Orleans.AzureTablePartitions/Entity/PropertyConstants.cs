
namespace Orleans.AzureTablePartitions
{
    public class PropertyConstants
    {
        private const int KB = 1024;

        /// <summary>
        /// Azure Table Storage supports a maximum of 64Kb but chars are UTF-16 so they are two bytes each.
        /// </summary>
        public const int MaxStringLength = 32 * KB;

        /// <summary>
        /// Azure Table Storage supports a maximum of 64Kb per column.
        /// </summary>
        public const int MaxPropertySize = 64 * KB;

        /// <summary>
        /// Azure Table Storage supports a maximum of 252 columns (+3 reserved properties: PK, RK, Timestamp)
        /// </summary>
        public const int MaxProperties = 252;

        /// <summary>
        /// The maximum length of a property name
        /// </summary>
        public const int MaxPropertyNameLength = 255;
    }
}
