hermes
======

Hermes is an multithreaded asynchronous job processing class. It allows you to register different job types and associate processor functions or classes with those job types.

Job processor function needs to expect job data as input. If a job processor is a class, it needs to have run() method that expects job data as input.

Jobs are discovered by watching a folder on the file system for new files. If a file is correctly formated it is parsed and processed passing the parsed data to the registered processor.

A file needs to be a pickled dictionary with a 'type' key matching one of the registered job types.

When the class is initialized it expects a path to the folder to watch. The folder needs to have two subfolders: 'in' for incoming jobs, and 'err' where failed jobs will be stored.

Usage
======

The usage is very simple. You need to define processors, initialize hermes with the path to the folder to watch, register processors with hermes and call start() method to start all the threads. By default hermes runs in interactive mode. If you want to run it in a daemon mode, just pass 'daemon=True' to the start() method.

```python
import hermes

class Download:
    def run(self, task):
        print("Downloading page: {0}".format(task['url']))
        return True

def send_email(task):
    print("Email for: {0}".format(task['rcpt']))
    return True

if __name__ == '__main__':
    hermes = hermes.Hermes('/tmp/jobs')
    hermes.register({'email':    send_email})
    hermes.register({'download': Download})
    hermes.start(daemon=True)
```

To send jobs, just pickle the dictionaries with the data for your jobs and save them into the defined folder. Hermes will pick it up from there.

```python
import cPickle

email = {'type':    'email',
         'from':    'no@mail.com',
         'rcpt':    'test@example.com',
         'subject': 'Test email',
         'body':    'Hi there!'}

download = {'type': 'download',
            'url':  'http://www.miljan.org/',
            'file': '/tmp/miljan.org.html'}

cPickle.dump(email, open("/tmp/jobs/in/email", "wb"))
cPickle.dump(download, open("/tmp/jobs/in/download", "wb"))
```
