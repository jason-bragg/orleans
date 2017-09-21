using Microsoft.WindowsAzure.Storage.Table;

namespace Orleans.Azure
{
    public interface IEntityConverter<TPoco>
        where TPoco : new()
    {
        TPoco ConvertFromStorage(DynamicTableEntity entity);

        DynamicTableEntity ConvertToStorage(TPoco poco);
    }
}
