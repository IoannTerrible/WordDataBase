using SimpleTextDatabase;

namespace WordDataBase
{
    public interface IDatabase
    {
        void BeginTransaction();
        void CommitTransaction();
        void RollbackTransaction();
        void CreateTable(string tableName, Column[] columns);
        void InsertData(string tableName, string[] data);
        IEnumerable<string[]> Select(
            string tableName,
            string[] columns = null,
            Func<string[], bool> filter = null,
            string orderByColumn = null
        );
        void DropTable(string tableName);
        void DropDatabase();
    }
}
