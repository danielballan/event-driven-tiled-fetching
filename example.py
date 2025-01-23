import sys
from functools import partial

import numpy
from event_model import DocumentRouter, RunRouter
from tiled.client import from_uri


URI = "https://tiled.nsls2.bnl.gov/api/v1/metadata/smi/raw"
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
        index = doc["seq_num"] - 1  # because I lost an argument with a scientist in 2015
        # TODO Backoff loop
        run = tiled_client[self.run_uid]
        array_client = run[self.stream_name, "data", self.field_name]
        image = array_client[index]
        do_science(index, image)


def build_stream_consumer(name, doc, *, start_doc, stream_name, field_name):
    "A run consumer consumes documents from one Stream of one Run"
    callbacks = []
    if doc["name"] == stream_name:
        callback = FetchImages(tiled_client=tiled_client, field_name=field_name)
        callback("start", start_doc)
        # RunRouter will feed callback the descriptor and subsequent documents.
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
    uid = "c21d472b-4241-4f48-8d62-a0b56e1d471d"  # from Dylan
    stream_name = "primary"
    field_name = "pil900KW_image"
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
