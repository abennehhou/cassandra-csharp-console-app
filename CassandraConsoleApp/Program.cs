using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using CassandraConsoleApp.Models;

namespace CassandraConsoleApp
{
    public class Program
    {
        private const string CassandraNodeAddress = "localhost";
        private const string KeyspaceName = "account_test";
        static void Main(string[] args)
        {
            // Define table mappings.
            MappingConfiguration.Global.Define<Mapping>();

            // Create a cluster instance using one cassandra node.
            var cluster = Cluster.Builder()
                .AddContactPoints(CassandraNodeAddress)
                .Build();

            // Create session to connect to the nodes using a keyspace.
            var session = cluster.Connect();
            session.CreateKeyspaceIfNotExists(KeyspaceName);
            session.ChangeKeyspace(KeyspaceName);

            // Delete all records
            var deleteResult = session.Execute($"DROP TABLE IF EXISTS {KeyspaceName}.{Mapping.AccountsTableName}");
            Console.WriteLine($"Table {Mapping.AccountsTableName} dropped: {deleteResult.IsFullyFetched}");

            // Re-create User Defined Types, used for nested objects
            var userDefinedTypeNameContact = nameof(Contact);
            var userDefinedTypeNameAccount = nameof(Account);
            DropType(session, userDefinedTypeNameAccount);
            DropType(session, userDefinedTypeNameContact);
            var createTypeContactResult = session.Execute($"CREATE TYPE {KeyspaceName}.contact (firstName text, lastName text);");
            Console.WriteLine($"Contact type created: {createTypeContactResult.IsFullyFetched}");
            var createTypeAccountResult = session.Execute($@"CREATE TYPE {KeyspaceName}.account ( 
                                                   id int, 
                                                   name text, 
                                                   tags list<text>,
                                                   contacts list<FROZEN<contact>>
                                                );");
            Console.WriteLine($"Account type created: {createTypeAccountResult.IsFullyFetched}");

            session.UserDefinedTypes.Define(UdtMap.For<Contact>(), UdtMap.For<Account>());

            // Create records
            var table = session.GetTable<Account>();
            table.CreateIfNotExists();
            Console.WriteLine($"Table {table.Name} created in keyspace {session.Keyspace}.");

            var records = GenerateRecordList(10000);
            InsertData(table, session, records);

            // Get some inserted records
            var existingRecords = table.Take(10).Execute().ToList();
            var countRecords = table.Count().Execute();
            Console.WriteLine($"{existingRecords.Count} existing records retrieved. Number of existing records: {countRecords}.");

            Console.ReadLine();
        }

        private static void DropType(ISession session, string typeName)
        {
            var result = session.Execute($"DROP TYPE IF EXISTS {typeName}");
            Console.WriteLine($"Type {typeName} dropped: {result.IsFullyFetched}");
        }

        private static void InsertData<T>(Table<T> table, ISession session, List<T> recordList)
        {
            // Insert data in a batch. There is a limit on batch operations, so we split the records on smaller lists.
            var watch = new Stopwatch();
            watch.Start();
            var recordsLists = SplitList(recordList, 100).ToList();
            foreach (var recordListInput in recordsLists)
            {
                var batch = session.CreateBatch();
                foreach (var record in recordListInput)
                {
                    batch.Append(table.Insert(record));
                }
                batch.Execute();
            }

            watch.Stop();

            Console.WriteLine("{0} records inserted. Elapsed: {1}.", recordList.Count, watch.Elapsed);
        }

        private static List<Account> GenerateRecordList(int size)
        {
            List<Account> records = new List<Account>();
            for (int i = 0; i < size; i++)
            {
                records.Add(new Account
                {
                    Id = i,
                    Name = $"One Piece {i}",
                    Tags = new List<string> { "Shonen", "Adventure" },
                    Contacts = new List<Contact>
                    {
                        new Contact
                        {
                            FirstName = "Monkey",
                            LastName = $"Luffy {i}"
                        },
                        new Contact
                        {
                            FirstName = "Nico",
                            LastName = $"Robin {i}"
                        }
                    }

                });
            }

            return records;
        }

        public static IEnumerable<List<T>> SplitList<T>(List<T> list, int size)
        {
            for (int i = 0; i < list.Count; i += size)
            {
                yield return list.GetRange(i, Math.Min(size, list.Count - i));
            }
        }
    }
}
