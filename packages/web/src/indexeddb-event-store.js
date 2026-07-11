class IndexedDBEventStore {
  constructor(options = {}) {
    this.adapter = options.adapter || createIndexedDBAdapter(options);
  }

  async push(event) {
    await this.adapter.push(event);
  }

  async peek(limit) {
    if (!Number.isInteger(limit) || limit < 0) {
      throw new Error("limit must be a non-negative integer");
    }
    return this.adapter.peek(limit);
  }

  async remove(count) {
    if (!Number.isInteger(count) || count < 0) {
      throw new Error("count must be a non-negative integer");
    }
    await this.adapter.remove(count);
  }

  async size() {
    return this.adapter.size();
  }

  async close() {
    if (typeof this.adapter.close === "function") await this.adapter.close();
  }
}

function createIndexedDBAdapter(options = {}) {
  const indexedDB = options.indexedDB || globalThis.indexedDB;
  if (!indexedDB || typeof indexedDB.open !== "function") {
    throw new Error("IndexedDB is unavailable; pass an indexedDB implementation or a store adapter");
  }
  const databaseName = options.databaseName || "openeventflow";
  const storeName = options.storeName || "events";
  const version = options.version || 1;
  let databasePromise;

  function database() {
    if (!databasePromise) {
      databasePromise = new Promise((resolve, reject) => {
        const request = indexedDB.open(databaseName, version);
        request.onupgradeneeded = () => {
          const db = request.result;
          if (!db.objectStoreNames.contains(storeName)) {
            db.createObjectStore(storeName, { keyPath: "sequence", autoIncrement: true });
          }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error || new Error("failed to open IndexedDB"));
        request.onblocked = () => reject(new Error("IndexedDB upgrade is blocked"));
      });
    }
    return databasePromise;
  }

  async function transaction(mode, operation) {
    const db = await database();
    const tx = db.transaction(storeName, mode);
    const completion = transactionCompletion(tx);
    let result;
    try {
      result = await operation(tx.objectStore(storeName));
    } catch (error) {
      try { tx.abort(); } catch (_) { /* transaction may already be inactive */ }
      throw error;
    }
    await completion;
    return result;
  }

  return {
    push(event) {
      return transaction("readwrite", (store) => requestResult(store.add({ event })));
    },
    peek(limit) {
      if (limit === 0) return [];
      return transaction("readonly", (store) => cursorValues(store.openCursor(), limit));
    },
    remove(count) {
      if (count === 0) return undefined;
      return transaction("readwrite", (store) => deleteCursorValues(store.openCursor(), count));
    },
    size() {
      return transaction("readonly", (store) => requestResult(store.count()));
    },
    async close() {
      if (!databasePromise) return;
      const db = await databasePromise;
      db.close();
      databasePromise = undefined;
    }
  };
}

function requestResult(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("IndexedDB request failed"));
  });
}

function transactionCompletion(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error || new Error("IndexedDB transaction failed"));
    transaction.onabort = () => reject(transaction.error || new Error("IndexedDB transaction aborted"));
  });
}

function cursorValues(request, limit) {
  return new Promise((resolve, reject) => {
    const values = [];
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor || values.length >= limit) {
        resolve(values);
        return;
      }
      values.push(cursor.value.event);
      cursor.continue();
    };
    request.onerror = () => reject(request.error || new Error("IndexedDB cursor failed"));
  });
}

function deleteCursorValues(request, count) {
  return new Promise((resolve, reject) => {
    let removed = 0;
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor || removed >= count) {
        resolve();
        return;
      }
      const deletion = cursor.delete();
      deletion.onerror = () => reject(deletion.error || new Error("IndexedDB deletion failed"));
      deletion.onsuccess = () => {
        removed += 1;
        cursor.continue();
      };
    };
    request.onerror = () => reject(request.error || new Error("IndexedDB cursor failed"));
  });
}

module.exports = { IndexedDBEventStore, createIndexedDBAdapter };
