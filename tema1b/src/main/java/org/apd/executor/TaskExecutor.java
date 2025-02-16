package org.apd.executor;

import org.apd.storage.EntryResult;
import org.apd.storage.SharedDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

public class TaskExecutor {
    private final SharedDatabase sharedDatabase;
    private final Semaphore[] indexSemaphores;
    private final ReentrantLock[] indexLocks;
    private final int[] activeReaders;
    private final int[] waitingWriters;

    public TaskExecutor(int storageSize, int blockSize, long readDuration, long writeDuration) {
            sharedDatabase = new SharedDatabase(storageSize, blockSize, readDuration, writeDuration);

            indexSemaphores = new Semaphore[storageSize];
            indexLocks = new ReentrantLock[storageSize];
            activeReaders = new int[storageSize];
            waitingWriters = new int[storageSize];

            for (int i = 0; i < storageSize; i++) {
                indexSemaphores[i] = new Semaphore(1);
                indexLocks[i] = new ReentrantLock();
                activeReaders[i] = 0;
                waitingWriters[i] = 0;
            }

        }

        public List<EntryResult> ExecuteWork(int poolSize, List<StorageTask> tasks, LockType lockType) {
            CustomThreadPool threadPool = new CustomThreadPool(poolSize);
            List<EntryResult> results = new ArrayList<>();
            ReentrantLock resultsLock = new ReentrantLock();

            for (StorageTask task : tasks) {
                threadPool.submit(() -> {
                    int index = task.index();

                    switch (lockType) {
                        case ReaderPreferred -> handleReaderPriority(task, index, results, resultsLock);
                        case WriterPreferred1 -> handleWriterPriority(task, index, results, resultsLock);
                        case WriterPreferred2 -> handleWriterPriorityWithMonitor(task, index, results, resultsLock);
                        default -> System.out.println("??? Shouldnt happen " + lockType);
                    }
                });
            }

            threadPool.shutdown();
            return results;
        }
    /*
    public List<EntryResult> ExecuteWorkSerial(List<StorageTask> tasks) {
        var results = tasks.stream().map(task -> {
            try {
                if (task.isWrite()) {
                    return sharedDatabase.addData(task.index(), task.data());
                } else {
                    return sharedDatabase.getData(task.index());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).toList();

        return results.stream().toList();
    }
    */
    private void handleReaderPriority(StorageTask task, int index, List<EntryResult> results, ReentrantLock resultsLock) {
        if (!task.isWrite()) {
            indexLocks[index].lock();
            try {
                if (activeReaders[index] == 0) {
                    indexSemaphores[index].acquire();
                }
                activeReaders[index]++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                indexLocks[index].unlock();
            }

            try {
                EntryResult result = sharedDatabase.getData(index);
                resultsLock.lock();
                try {
                    results.add(result);
                } finally {
                    resultsLock.unlock();
                }
            } finally {
                indexLocks[index].lock();
                try {
                    activeReaders[index]--;
                    if (activeReaders[index] == 0) {
                        indexSemaphores[index].release();
                    }
                } finally {
                    indexLocks[index].unlock();
                }
            }
        } else {
            try {
                indexSemaphores[index].acquire();
                EntryResult result = sharedDatabase.addData(index, task.data());
                resultsLock.lock();
                try {
                    results.add(result);
                } finally {
                    resultsLock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                indexSemaphores[index].release();
            }
        }
    }
    public void handleWriterPriorityWithMonitor(StorageTask task, int index, List<EntryResult> results, ReentrantLock resultsLock) {
        if (task.isWrite()) {
            synchronized (indexLocks[index]) {
                waitingWriters[index]++;

                while (activeReaders[index] > 0) {
                    try {
                        indexLocks[index].wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        waitingWriters[index]--;
                        return;
                    }
                }

                try {
                    EntryResult result = sharedDatabase.addData(index, task.data());
                    resultsLock.lock();
                    try {
                        results.add(result);
                    } finally {
                        resultsLock.unlock();
                    }

                } finally {
                    waitingWriters[index]--;
                    indexLocks[index].notifyAll();
                }
            }
        } else {
            synchronized (indexLocks[index]) {
                while (waitingWriters[index] > 0) {
                    try {
                        indexLocks[index].wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                activeReaders[index]++;
            }

            try {
                EntryResult result = sharedDatabase.getData(index);
                resultsLock.lock();
                try {
                    results.add(result);
                } finally {
                    resultsLock.unlock();
                }
            } finally {
                synchronized (indexLocks[index]) {
                    activeReaders[index]--;
                    if (activeReaders[index] == 0) {
                        indexLocks[index].notifyAll();
                    }
                }
            }
        }
    }
    private void handleWriterPriority(StorageTask task, int index, List<EntryResult> results, ReentrantLock resultsLock) {
        if (task.isWrite()) {
            indexLocks[index].lock();
            try {
                waitingWriters[index]++;
            } finally {
                indexLocks[index].unlock();
            }

            try {
                indexSemaphores[index].acquire();

                while (true) {
                    indexLocks[index].lock();
                    try {
                        if (activeReaders[index] == 0) break;
                    } finally {
                        indexLocks[index].unlock();
                    }
                }

                EntryResult result = sharedDatabase.addData(index, task.data());
                resultsLock.lock();
                try {
                    results.add(result);
                } finally {
                    resultsLock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                indexLocks[index].lock();
                try {
                    waitingWriters[index]--;
                } finally {
                    indexLocks[index].unlock();
                }
                indexSemaphores[index].release();
            }
        } else {
            boolean canRead = false;
            while (!canRead) {
                indexLocks[index].lock();
                try {
                    if (waitingWriters[index] == 0) {
                        activeReaders[index]++;
                        canRead = true;
                    }
                } finally {
                    indexLocks[index].unlock();
                }
            }

            try {
                EntryResult result = sharedDatabase.getData(index);
                resultsLock.lock();
                try {
                    results.add(result);
                } finally {
                    resultsLock.unlock();
                }
            } finally {
                indexLocks[index].lock();
                try {
                    activeReaders[index]--;
                } finally {
                    indexLocks[index].unlock();
                }
            }
        }
    }
    private static class CustomThreadPool {
        private final List<WorkerThread> workers = new ArrayList<>();
        private final CustomTaskQueue taskQueue = new CustomTaskQueue();
        private volatile boolean isShutdown = false;

        public CustomThreadPool(int poolSize) {
            for (int i = 0; i < poolSize; i++) {
                WorkerThread worker = new WorkerThread(taskQueue);
                workers.add(worker);
                worker.start();
            }
        }

        public void submit(Runnable task) {
            if (isShutdown) {
                throw new IllegalStateException("Threadpol is shutdown.");
            }
            taskQueue.addTask(task);
        }

        public void shutdown() {
            isShutdown = true;
            taskQueue.shutdown();
            for (WorkerThread worker : workers) {
                try {
                    worker.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static class WorkerThread extends Thread {
        private final CustomTaskQueue taskQueue;

        public WorkerThread(CustomTaskQueue taskQueue) {
            this.taskQueue = taskQueue;
        }

        @Override
        public void run() {
            while (true) {
                Runnable task = taskQueue.getTask();
                if (task == null) {
                    break;
                }
                task.run();
            }
        }
    }
    private static class CustomTaskQueue {
        private final List<Runnable> taskQueue = new ArrayList<>();
        private volatile boolean isShutdown = false;

        public void addTask(Runnable task) {
            synchronized (taskQueue) {
                if (isShutdown) {
                    throw new IllegalStateException("Threadpool is shutdown");
                }
                taskQueue.add(task);
                taskQueue.notify();
            }
        }

        public Runnable getTask() {
            synchronized (taskQueue) {
                while (taskQueue.isEmpty() && !isShutdown) {
                    try {
                        taskQueue.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (taskQueue.isEmpty()) {
                    return null;
                }
                return taskQueue.remove(0);
            }
        }

        public void shutdown() {
            synchronized (taskQueue) {
                isShutdown = true;
                taskQueue.notifyAll();
            }
        }
    }
}

