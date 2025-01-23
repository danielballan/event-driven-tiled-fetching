import sys
import time
from functools import partial

import numpy
from event_model import DocumentRouter, RunRouter
from tiled.client import from_uri


URI = "https://tiled.nsls2.bnl.gov/api/v1/metadata/smi/raw"
BACKOFF = [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8]  # delay in seconds
tiled_client = from_uri(URI)

def do_science(index, image):
    "Callback that receives image and does science"
    print(f"Total Intensity of Image {index:05}: {numpy.sum(image)}")


class FetchImages(DocumentRouter):
    "Fetches images from Tiled when Events are received."
    def __init__(self, *args, tiled_client, field_name, **kwargs):
        self.field_name = field_name
        super().__init__(*args, **kwargs)

    def start(self, doc):
        self.run_uid = doc["uid"]
        print(f"Processing Run {self.run_uid}.", file=sys.stderr)

    def descriptor(self, doc):
        self.stream_name = doc["name"]

    def event(self, doc):
        run = tiled_client[self.run_uid]
        array_client = run[self.stream_name, "data", self.field_name]
        index = doc["seq_num"] - 1  # because I lost an argument with a scientist in 2015
        for delay in BACKOFF:
            try:
                image = array_client[index]
            except IndexError:
                # Try again after a delay.
                time.sleep(delay)
            else:
                # Success! Escape retry loop.
                break
        else:
            # Out of retries
            raise TimeoutError("Index {index} could not be retrieved")
        do_science(index, image)


def build_stream_consumer(name, doc, *, start_doc, stream_name, field_name):
    "A run consumer consumes documents from one Stream of one Run"
    callbacks = []
    if doc["name"] == stream_name:
        callback = FetchImages(tiled_client=tiled_client, field_name=field_name)
        callbacks.append(callback)
    return callbacks


def build_run_consumer(name, doc, *, stream_name, field_name):
    "A run consumer consumes documents from one Run"
    if name != "start":
        raise ValueError("This should only ever receive a 'start' doc.")
    factories= []
    subfactories = [
        partial(
            build_stream_consumer,
            start_doc=doc,
            stream_name=stream_name,
            field_name=field_name,
        )
    ]
    return (factories, subfactories)




def main():
    "Demo"
    # Constants point to an example dataset.
    uid = "c21d472b-4241-4f48-8d62-a0b56e1d471d"  # from Dylan
    stream_name = "primary"
    field_name = "pil900KW_image"

    # Construct callbacks that will consume document stream.
    factory = partial(
        build_run_consumer,
        stream_name=stream_name,
        field_name=field_name,
    )
    run_router = RunRouter([factory])

    # In place of consuming live from a message bus...
    #
    #     kafka_consumer.subscribe(run_router)
    #
    # fetch the documents from tiled and feed them into the run_router.
    doc_gen = tiled_client[uid].documents()
    print("Streaming documents...", file=sys.stderr)
    for name, doc in doc_gen:
        run_router(name, doc)
    print("Stream complete.", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass  # exit cleanly
