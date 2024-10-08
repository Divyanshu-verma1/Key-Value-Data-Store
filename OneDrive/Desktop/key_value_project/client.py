# client.py

from kv_store import KeyValueStore

def main():
    # Initialize the KeyValueStore
    kv_store = KeyValueStore()

    # Create a key-value pair with TTL of 60 seconds
    kv_store.create('key1', {'name': 'Ram'}, ttl=10)
    print("Created 'key1'")

    # Read the value of 'key1'
    try:
        value = kv_store.read('key1')
        print(f"Read 'key1': {value}")
    except KeyError as e:
        print(e)

    # Wait for TTL to expire (optional, for testing TTL)
    # time.sleep(11)

    # Delete 'key1'
    try:
        kv_store.delete('key1')
        print("Deleted 'key1'")
    except KeyError as e:
        print(e)

    # Batch create multiple key-value pairs
    kv_store.batch_create({
        'key2': {'city': 'Bengalore'},
        'key3': {'city': 'Hydrabad'}
    }, ttl=120)
    print("Batch created 'key2' and 'key3'")

    # Read 'key2'
    try:
        value = kv_store.read('key2')
        print(f"Read 'key2': {value}")
    except KeyError as e:
        print(e)

    # Try deleting a non-existent key
    try:
        kv_store.delete('key4')
    except KeyError as e:
        print(e)

if __name__ == '__main__':
    main()
