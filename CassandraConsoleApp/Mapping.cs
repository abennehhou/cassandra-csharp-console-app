using Cassandra;
using Cassandra.Mapping;
using CassandraConsoleApp.Models;

namespace CassandraConsoleApp
{
    public class Mapping : Mappings
    {
        public const string AccountsTableName = "accounts";

        public Mapping()
        {
            For<Account>()
                .TableName(AccountsTableName)
                .PartitionKey(u => u.Id)
                .Column(c => c.Contacts, map => map.AsFrozen()); // Nested objects must be specified as Frozen.


        }
    }
}
