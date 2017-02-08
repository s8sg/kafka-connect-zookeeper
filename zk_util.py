# upload_to_zk:
# Unitility to upload/download data to/from zk
from kazoo.client import KazooClient
import sys

class UploadToZK( object ):
    
    def __init__(self, zk_servers):
        if isinstance(zk_servers, list):
            self.zk_servers = ",".join(zk_servers)
        self.zk_clients = KazooClient(hosts=self.zk_servers)
        self.zk_clients.start()
        
    def upload_to_zk(self, node, data):
        if self.zk_clients.exists(node, watch=None) is None:
            self.zk_clients.create(node, data, acl=None, ephemeral=False, sequence=False, makepath=True)
        self.zk_clients.set(node, data)
        
    def is_path_exist(self, node):
        if self.zk_clients.exists(node, watch=None) is None:
            return False
        return True
    
    def close():
        self.zk_clients.close()

class LoadFromZK( object ):
    
    def __init__(self, zk_servers):
        if isinstance(zk_servers, list):
            self.zk_servers = ",".join(zk_servers)
        self.zk_clients = KazooClient(hosts=self.zk_servers)
        self.zk_clients.start()
        
    def load_from_zk(self, node, data):
        if self.zk_clients.exists(node, watch=None) is None:
            self.zk_clients.create(node, data, acl=None, ephemeral=False, sequence=False, makepath=True)
        self.zk_clients.set(node, data)
        
    def is_path_exist(self, node):
        if self.zk_clients.exists(node, watch=None) is None:
            return False
        return True

    def close():
        self.zk_clients.close()

def load_data_from_data_source(source):
    data = source  
    if source.startswith("file:"):
        with open(source.split(":")[1], 'rb') as file:
            data = file.read().replace('\n', '')
    return data

def unload_data_to_data_source(data_source, data):
    if data_source == "stdout":
        print data
    elif data_source.startswith("file:"):
        with open(source.split(":")[1], 'w') as file:
            data = file.write(data)
    else:
        print "Invalid data source: %s" % data_source
        print_usage()
        exit(1)

        
def print_usage(name="upload_to_zk.py"):
    usage = "usage: %s <operation> <zk_hosts> <zk_node_path> <data_source> \n" % name + \
            "       operation   : upload | download\n" + \
            "       data_source : file:<file_name> | <inline_data>"
    print(usage)    

# __main__
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print_usage(sys.argv[0])
        exit(1)
    operation = sys.argv[1]
    zk_servers = sys.argv[2]
    zk_node = sys.argv[3]
    data_source = ""
    try: 
        data_source = sys.argv[4]
    except:
        data_source = "stdout"
    if operation == "download":
        print "Unloading data from zk_node to source"
        load_from_zk = LoadFromZK(zk_servers.split(","))
        data = load_from_zk.load_from_zk(zk_node)
        if data is None:
            print "Failed to load data from zk:%s !" % zk_node
            exit(1)
        print "Unloading Node data to: %s" % data_source
        unload_data_to_data_source(data_source, data)
        print "Successfully downloaded data to: %s" % data_source
        load_from_zk.close()
    elif operation == "upload":    
        print "Loading data from source to zk_node"
        data = load_data_from_data_source(data_source)
        if data is None:
            print "Failed to load data from source !"
            exit(1)
        print "Initiating Connection to zk servers: %s" % zk_servers
        upload_to_zk = UploadToZK(zk_servers.split(","))
        print "Uploading Node data to ZK node: %s" % zk_node
        upload_to_zk.upload_to_zk(zk_node, data)
        print "Verifying upload..."
        if not upload_to_zk.is_path_exist(zk_node):
            print "Failed to verify the node: %s" % zk_node
        print "Successfully uploaded data at: zk:%s" % zk_node
        load_from_zk.close()
    else:
        print "Invalid operation: %s" % operation
        print_usage(sys.argv[0])
        exit(1)
    exit(0)
