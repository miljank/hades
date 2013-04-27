import os
import sys
import Queue
import signal
import inspect
import cPickle
import threading
from   time      import sleep
from   pyinotify import Notifier, EventsCodes, WatchManager, ProcessEvent

class Hermes:
    """Hermes is a job processing class. It watches a folder for new jobs and
    runs a registered processor for that specific job type.

    A processor can be either a function or class. A processor needs to return
    True or False.

    If the processor is a function it must accept the content of the job as an input.

    If the processor is a class it needs to have run() method that will
    accept the content of the job as an input. E.g. Processor().run(job)

    If a task fails it will be reprocessed and eventually moved to the error folder."""
    def __init__(self, folder, threads=5, retries=3, save_failed=True):
        # Signals if the threads should exit or not
        self.run         = True
        # Queues
        self.qin         = Queue.Queue()
        self.qerr        = Queue.Queue()
        # Watch objects
        self.wm          = WatchManager()
        self.mask        = EventsCodes.FLAG_COLLECTIONS['OP_FLAGS']['IN_CREATE']
        # Pool of threads
        self.tpool       = []
        # Parent folder to watch
        self.folder      = folder
        # Number of threads to start
        self.threads     = threads
        # Number of times to reprocess failed task
        self.retries     = retries - 1
        # The list of registered processors
        self.processors  = {}
        # Save failed tasks
        self.save_failed = save_failed

        # Folders
        self.in_dir  = os.path.join(self.folder, "in")
        self.err_dir = os.path.join(self.folder, "err")

    def __parse_task(self, file_name):
        """Unpickles a job file and verifies that it contains
        a dictionary with a registered job type"""
        task_file = os.path.join(self.in_dir, file_name)
        try:
            task = cPickle.load(open(task_file, "rb"))
        except:
            return False

        if not isinstance(task, dict) or not 'type' in task:
            return False

        if task['type'] not in self.processors:
            return False

        return task

    def __process_task(self, task):
        """Runs a registered processor for that specific job type"""
        processor = self.processors[task['type']]
        if inspect.isfunction(processor):
            return processor(task)
        elif inspect.isclass(processor):
            return processor().run(task)
        else:
            self.qerr.put(task)
            return True

    def __process_failed_task(self):
        """Saves or removes failed tasks"""
        while self.run:
            # Block for one second
            try:
                task = self.qerr.get(True, 1)
            except Queue.Empty:
                continue

            source = os.path.join(self.in_dir,  task["file"])
            if self.save_failed:
                destination = os.path.join(self.err_dir, task["file"])
                os.rename(source, destination)
            else:
                os.remove(source)

    def __process_queue(self):
        """Unpickle the content and process the job."""
        while self.run:
            # Block for one second
            try:
                job  = self.qin.get(True, 1)
            except Queue.Empty:
                continue

            task = self.__parse_task(job["file"])
            if task:
                if self.__process_task(task):
                    os.remove(os.path.join(self.in_dir, job["file"]))
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

            self.qerr.put(job)
            self.qin.task_done()

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
            # Create a thread
            thread = threading.Thread(target=task)
            # Start a thread
            thread.start()
            # Save thread reference
            self.tpool.append(thread)

    def __start_daemon(self):
        """Forks a daemon process and starts all the threads"""
        # Fork a child and exit
        try:
            pid = os.fork()
            if pid > 0:
                os._exit(0)
        except OSError, e:
            print("error: Fork #1 failed: %d (%s)" % (e.errno, e.strerror))
            sys.exit(1)

        # We are now in the child
        # Release file handlers and create new session
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # We need to fork again
        # otherwise, zombie processes might be created
        try:
            pid = os.fork()
            if pid > 0:
                os._exit(0)
        except OSError, e:
            print("error: Fork #2 failed: %d (%s)" % (e.errno, e.strerror))
            sys.exit(2)

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

    def register(self, task):
        """Register a job type with the processor."""
        self.processors.update(task)

    def start(self, daemon=False):
        if daemon:
            self.__start_daemon()
        else:
            self.__start_interactive()

    class WatchForChange(ProcessEvent):
        """Watches a folder for new files and starts a job processing."""
        def __init__(self, hermes):
            self.hermes = hermes

        def process_IN_CREATE(self, event):
            sleep(.1)
            self.hermes.qin.put({"retries": self.hermes.retries, "file": event.name})
