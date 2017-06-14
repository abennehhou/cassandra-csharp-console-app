using System;
using System.Collections.Generic;

namespace CassandraConsoleApp.Models
{
    public class Account
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public IEnumerable<string> Tags { get; set; }

        public IEnumerable<Contact> Contacts { get; set; } 
    }
}
