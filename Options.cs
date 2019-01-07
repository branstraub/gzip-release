using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace gzip
{
    class Options
    {
        public string BlobConnectionStringSource { get; set; }
        public string BlobConnectionStringDestination { get; set; }
        public string QueueConnectionString { get; set; }
        public string QueueName { get; set; }

    }

}
