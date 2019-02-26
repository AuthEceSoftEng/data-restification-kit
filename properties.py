from RetrieveService.properties import retrieve_service_host, retrieve_service_port
from ImportService.properties import import_service_host, import_service_port

# Set path to python executable
python_exec_path = 'python3'

retrieve_service_access_url = "http://" + retrieve_service_host + ":" + str(retrieve_service_port) + "/api/v1"
import_service_access_url = "http://" + import_service_host + ":" + str(import_service_port) + "/api/v1"

# Set main service host
main_service_host = '127.0.0.1'

# Set main service port
main_service_port = 5060