# kv_store.py

import json
import os
import time
import threading

class KeyValueStore:
    def __init__(self, file_path='kv_store.json', max_file_size=1 * 1024 * 1024 * 1024):
        """
        Initialize the key-value store.
        :param file_path: The file path where key-value data will be stored.
        :param max_file_size: Maximum size for the file (default: 1GB).
        """
        self.file_path = file_path
        self.max_file_size = max_file_size
        self.store = {}
        self.ttl_store = {}
        self.lock = threading.Lock()

        # Load data from the file if it exists, else create a new file
        if os.path.exists(self.file_path):
            self._load_store()
        else:
            self._save_store()

    def _load_store(self):
        """Load the key-value data from the file."""
        with open(self.file_path, 'r') as file:
            data = json.load(file)
            self.store = data.get('store', {})
            self.ttl_store = data.get('ttl_store', {})

    def _save_store(self):
        """Save the current key-value data to the file."""
        with open(self.file_path, 'w') as file:
            json.dump({'store': self.store, 'ttl_store': self.ttl_store}, file)

    def _check_file_size(self):
        """Ensure the file size does not exceed 1GB."""
        if os.path.exists(self.file_path) and os.path.getsize(self.file_path) > self.max_file_size:
            raise MemoryError("File size exceeds the 1GB limit.")

    def _is_key_expired(self, key):
        """Check if the TTL for the key has expired."""
        if key in self.ttl_store:
            return time.time() > self.ttl_store[key]
        return False

    def _clean_expired_keys(self):
        """Remove expired keys from the store."""
        expired_keys = [key for key in self.ttl_store if self._is_key_expired(key)]
        for key in expired_keys:
            del self.store[key]
            del self.ttl_store[key]
        self._save_store()

    def create(self, key, value, ttl=None):
        """
        Create a new key-value pair with an optional TTL.
        :param key: The key (string, max length 32 characters).
        :param value: The value (JSON, max size 16KB).
        :param ttl: Optional Time-to-Live in seconds.
        """
        if len(key) > 32:
            raise ValueError("Key length exceeds 32 characters.")
        if len(json.dumps(value)) > 16 * 1024:
            raise ValueError("Value exceeds 16KB size limit.")

        with self.lock:
            self._clean_expired_keys()

            if key in self.store:
                raise KeyError(f"Key '{key}' already exists. Use a unique key or delete the existing one.")

            self.store[key] = value

            if ttl:
                self.ttl_store[key] = time.time() + ttl

            self._save_store()
            self._check_file_size()

    def read(self, key):
        """
        Retrieve the value for a given key.
        :param key: The key to retrieve.
        :return: The value, or None if the key does not exist or has expired.
        """
        with self.lock:
            self._clean_expired_keys()

            if key not in self.store:
                raise KeyError(f"Key '{key}' not found or has expired.")
            return self.store[key]

    def delete(self, key):
        """
        Delete a key-value pair.
        :param key: The key to delete.
        """
        with self.lock:
            self._clean_expired_keys()

            if key not in self.store:
                raise KeyError(f"Key '{key}' not found.")

            del self.store[key]
            if key in self.ttl_store:
                del self.ttl_store[key]

            self._save_store()

    def batch_create(self, items, ttl=None):
        """
        Create multiple key-value pairs in a single batch operation.
        :param items: A dictionary of key-value pairs.
        :param ttl: Optional TTL for all keys.
        """
        if len(items) > 100:
            raise ValueError("Batch size exceeds limit of 100 items.")

        with self.lock:
            self._clean_expired_keys()

            for key, value in items.items():
                if len(key) > 32:
                    raise ValueError(f"Key '{key}' exceeds 32 characters.")
                if len(json.dumps(value)) > 16 * 1024:
                    raise ValueError(f"Value for key '{key}' exceeds 16KB.")
                if key in self.store:
                    raise KeyError(f"Key '{key}' already exists. Use a unique key or delete the existing one.")

                self.store[key] = value

                if ttl:
                    self.ttl_store[key] = time.time() + ttl

            self._save_store()
            self._check_file_size()
