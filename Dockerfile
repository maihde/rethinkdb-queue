FROM rethinkdb

RUN \
    apt-get update && \
    apt-get install -y python-pip python && \
    rm -rf /var/lib/apt/lists/*

# install python driver for rethinkdb
RUN pip install rethinkdb

ADD worker.py /usr/local/bin/worker.py
