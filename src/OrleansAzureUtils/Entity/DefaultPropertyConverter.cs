
namespace Orleans.Azure
{
    public class DefaultPropertyConverter : JsonPropertyConverter
    {
        public static IPropertyConverter Instance { get; } = new DefaultPropertyConverter();
    }

}
