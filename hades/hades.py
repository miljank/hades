import os
import sys
import json
import Queue
import signal
import inspect
import threading
from time import sleep
from pyinotify import Notifier, EventsCodes, WatchManager, ProcessEvent


class Hades:
    """Hades is a job processing class. It watches a folder for new jobs and
    runs a registered processor for that specific job type.

    A processor can be either a function or class. A processor needs to return
    True or False.

    If the processor is a function it must accept the content of the job as an input.

    If the processor is a class it needs to have run() method that will
    accept the content of the job as an input. E.g. Processor().run(job)

    If a task fails it will be reprocessed and eventually moved to the error folder."""
    def __init__(self, folder, threads=5, retries=3, save_successful=True, save_failed=True):
        # Signals if the threads should exit or not
        self.run = True
        # Queues
        self.qin = Queue.Queue()
        self.qerr = Queue.Queue()
        # Watch objects
        self.wm = WatchManager()
        self.mask = EventsCodes.FLAG_COLLECTIONS['OP_FLAGS']['IN_CREATE']
        # Parent folder to watch
        self.folder = folder
        # Number of threads to start
        self.threads = threads
        # Number of times to reprocess failed task
        self.retries = retries - 1
        # The list of registered processors
        self.processors = {}
        # Save failed tasks
        self.save_failed = save_failed
        self.save_successful = save_successful

        # Folders
        self.in_dir = os.path.join(self.folder, "in")
        self.cur_dir = os.path.join(self.folder, "cur")
        self.err_dir = os.path.join(self.folder, "err")

    def __parse_task(self, file_name):
        """Serializes a job file and verifies that it contains
        a dictionary with a registered job type"""
        task_file = os.path.join(self.in_dir, file_name)
        if not os.path.isfile(task_file):
            return False

        try:
            task = json.load(open(task_file))
        except ValueError:
            return False

        if not isinstance(task, dict) or not 'type' in task:
            return False

        if task['type'] not in self.processors:
            return False

        return task

    def __wait_for_child(self, pid):
        rc = os.waitpid(pid, os.WNOHANG)
        while rc == (0, 0):
            if not self.run:
                os.kill(pid, signal.SIGTERM)
                os.waitpid(pid, 0)
                return False

            sleep(.1)
            rc = os.waitpid(pid, os.WNOHANG)
        return rc[1]

    def __process_task(self, task):
        """Runs a registered processor for that specific job type"""
        pid = os.fork()
        if pid == 0:
            processor = self.processors[task['type']]
            if inspect.isfunction(processor):
                os._exit(0) if processor(task) else os._exit(1)
            os._exit(0) if processor().run(task) else os._exit(1)
        else:
            rc = self.__wait_for_child(pid)
            return True if rc == 0 else False

    def __process_failed_task(self):
        """Saves or removes failed tasks"""
        while self.run:
            # Block for one second
            try:
                task = self.qerr.get(True, 1)
            except Queue.Empty:
                continue

            source = os.path.join(self.in_dir,  task["file"])
            if not os.path.isfile(source):
                return False

            if self.save_failed:
                destination = os.path.join(self.err_dir, task["file"])
                os.rename(source, destination)
            else:
                os.remove(source)

    def __process_queue(self):
        """Serializes the content and process the job."""
        while self.run:
            # Block for one second
            try:
                job = self.qin.get(True, 1)
            except Queue.Empty:
                continue

            task = self.__parse_task(job["file"])
            if not task:
                self.qerr.put(job)
                self.qin.task_done()
                continue

            if self.__process_task(task):
                if self.save_successful:
                    source = os.path.join(self.in_dir,  job["file"])
                    destination = os.path.join(self.cur_dir, job["file"])
                    os.rename(source, destination)
                else:
                    os.remove(source)
                self.qin.task_done()
                continue

            if job["retries"] < 1:
                self.qerr.put(job)
                self.qin.task_done()
                continue

            job["retries"] = job["retries"] - 1
            self.qin.put(job)
            self.qin.task_done()
            continue

    def __watcher(self):
        """Starts the folder watcher"""
        self.wm.add_watch(self.in_dir, self.mask)
        self.notifier = Notifier(self.wm, self.WatchForChange(self))

        while self.run:
            self.notifier.process_events()
            if self.notifier.check_events(1000):
                self.notifier.read_events()

        self.notifier.stop()

    def __start_threads(self, task, number_of_threads=5):
        """Starts the threads and adds them to the pool"""
        for i in range(number_of_threads):
            thread = threading.Thread(target=task)
            thread.start()

    def __fork(self):
        """Forks a child process and stops the parent"""
        try:
            pid = os.fork()
            if pid > 0:
                os._exit(0)
        except OSError, e:
            print("error: Fork failed: {0} ({1})".format(e.errno, e.strerror))
            sys.exit(1)

    def __start_daemon(self):
        """Forks a daemon process and starts all the threads"""
        # Fork a child and exit
        self.__fork()

        # We are now in the child
        # Release file handlers and create new session
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # We need to fork again
        # otherwise, zombie processes might be created
        self.__fork()

        self.__start_all_workers()
        while self.run:
            sleep(60)

    def __start_interactive(self):
        """Starts all the threads in interactive mode"""
        self.__start_all_workers()
        while self.run:
            try:
                sleep(60)
            except KeyboardInterrupt:
                self.run = False
                sys.exit(0)

    def __signal_handler(self, signal, frame):
        """Signals threads to stop in case a SIGTERM is received"""
        # SIGTERM
        if signal is 15:
            self.run = False
            sys.exit(0)

    def __start_all_workers(self):
        """Starts the watcher and processing threads"""
        signal.signal(signal.SIGTERM, self.__signal_handler)

        self.__start_threads(self.__watcher,             number_of_threads=1)
        self.__start_threads(self.__process_queue,       number_of_threads=self.threads)
        self.__start_threads(self.__process_failed_task, number_of_threads=1)

    class WatchForChange(ProcessEvent):
        """Watches a folder for new files and starts a job processing."""
        def __init__(self, hades):
            self.hades = hades

        def process_IN_CREATE(self, event):
            sleep(.1)
            self.hades.qin.put({"retries": self.hades.retries, "file": event.name})

    def register(self, task):
        """Register a job type with the processor."""
        for key, value in task.items():
            if not inspect.isclass(value) and \
               not inspect.isfunction(value):
                raise ValueError("Processor for the task {0} is not a class or a function.".format(key))

            self.processors.update({key: value})

    def start(self, daemon=False):
        if daemon:
            self.__start_daemon()
        else:
            self.__start_interactive()
