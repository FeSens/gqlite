import ctypes
import json

# Load the shared library
lib = ctypes.CDLL('./libgqlite.so')  # Adjust path if needed

# Define GraphDB pointer type
class GraphDB(ctypes.c_void_p):
    pass

# Function prototypes
lib.graphdb_open.restype = GraphDB
lib.graphdb_open.argtypes = [ctypes.c_char_p]

lib.graphdb_close.argtypes = [GraphDB]

lib.execute_cypher.restype = ctypes.c_void_p
lib.execute_cypher.argtypes = [GraphDB, ctypes.c_char_p]

lib.free_cypher_result.argtypes = [ctypes.c_void_p]

lib.cypher_result_to_d3_json.restype = ctypes.c_char_p
lib.cypher_result_to_d3_json.argtypes = [ctypes.c_void_p]

lib.free_d3_json.argtypes = [ctypes.c_char_p]

class GQLiteDB:
    def __init__(self, path):
        self.db = lib.graphdb_open(path.encode('utf-8'))
        if not self.db:
            raise RuntimeError("Failed to open database")

    def __del__(self):
        if hasattr(self, 'db') and self.db:
            lib.graphdb_close(self.db)

    def execute_query(self, query):
        res_ptr = lib.execute_cypher(self.db, query.encode('utf-8'))
        json_str_ptr = lib.cypher_result_to_d3_json(res_ptr)
        json_str = ctypes.string_at(json_str_ptr, -1).decode('utf-8')  # NULL-terminated
        # lib.free_cypher_result(res_ptr)
        # lib.free_d3_json(json_str_ptr)
        return json.loads(json_str)