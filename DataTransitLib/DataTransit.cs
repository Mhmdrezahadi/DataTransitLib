using EFCore.BulkExtensions;
using GemBox.Spreadsheet;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;

namespace DataTransitLib
{
    public static class DataTransit
    {
        public static async Task AddExcelDataToRedis(Stream file, string redisConnectionString, string listName)
        {
            // Connect to redis
            var _redisDb = ConnectionMultiplexer.Connect(redisConnectionString).GetDatabase();
            ExcelFile workbook = ExcelFile.Load(file);
            ExcelWorksheet worksheet = workbook.Worksheets[0];

            // Iterate through all rows in worksheet 0 and add to redis list.
            foreach (ExcelRow row in worksheet.Rows)
            {
                string value = row.Cells[0].Value?.ToString() ?? "EMPTY";
                await _redisDb.ListLeftPushAsync(listName, value, When.Always, CommandFlags.FireAndForget);
            }
        }
        public static async Task AddRedisDataToSql(this DbContext dbContext, string redisConnectionString, string listName, string tableName)
        {
            // Connect to redis
            var db = ConnectionMultiplexer.Connect(redisConnectionString).GetDatabase();
            var server = ConnectionMultiplexer.Connect(redisConnectionString).GetServer(redisConnectionString);

            // get all elements of list with name
            var ls = await db.ListRangeAsync(listName, 0, -1);

            // find database table model in assemblies
            var type = (from assembly in AppDomain.CurrentDomain.GetAssemblies()
                        from t in assembly.GetTypes()
                        where t.Name == tableName
                        select t).FirstOrDefault();

            // create and assign list of database model
            List<object> myobjs = new();
            foreach (var item in ls)
            {
                var obj = Activator.CreateInstance(type, Guid.NewGuid(), item.ToString());
                myobjs.Add(obj);
            }
            // insert list to sql using bulkInsert
            await dbContext.BulkInsertAsync(myobjs);
        }
    }
}