import os
import sys
import signal
import gi
import argparse
from pathlib import Path
import multiprocessing
sys.path.append("/opt/openGateExtensions/") # needed to load package below
import openGateMqttPython as pyMqtt

gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import Gst, GLib

DESCRIPTION = """
The application receives an RTSP stream as source, decodes it, uses YOLOv8
TFLite model to identify the object in the scene from the camera stream and
returns all detections for each frame as a Python list.
"""

DEFAULT_RTSP_SRC = "rtsp://127.0.0.1:8554/imagestream" # for right now assume localhost will resolve

# Configurations for Detection (May need to be changed for each model configured)
DEFAULT_DETECTION_MODEL = "/etc/models/yolov8_det.tflite"
DEFAULT_DETECTION_MODULE = "yolov8"
DEFAULT_DETECTION_LABELS = "/etc/labels/yolov8n.labels"
DEFAULT_DETECTION_CONSTANTS = "YoloV8,q-offsets=<-33.0,0.0,0.0>,\
    q-scales=<3.2430853843688965,0.0037704326678067446,1.0>;"


eos_received = False

sampleQueue = multiprocessing.Queue()
# Author Qualcomm
def create_element(factory_name, name):
    """Create a GStreamer element."""
    element = Gst.ElementFactory.make(factory_name, name)
    if not element:
        raise Exception(f"Failed to create {factory_name} named {name}!")
    return element

# Author Qualcomm
def link_elements(link_orders, elements):
    """Link elements in the specified orders."""
    for link_order in link_orders:
        src = None
        for element in link_order:
            dest = elements[element]
            if src and not src.link(dest):
                raise Exception(
                    f"Failed to link element {src.get_name()} with {dest.get_name()}"
                )
            src = dest


# Author Andrew Pegg
def processSample(queue:multiprocessing.Queue):
    # process text sample here (no sure of the format yet)
    # if no objects of interest or empty string return
    # check if MQTT config exist and if we should create a client
    mqttClient = None
    if os.path.exists("/etc/mqtt_config.txt"):
        url = Path("/etc/mqtt_config.txt").read_text()
        mqttClient = pyMqtt.MQTTClient(url,"openGateClient")
        mqttClient.connect()
    # may want to do something similar for your stuff Daniil of checking for url for automation
    while True:
        text_input = queue.get() # blocks until sample ready
        if text_input == '':
            continue
        # TODO process text sample here
        continue # remove this once sample processing code is ready
        # TODO detect objects of interest (DANIIL)

        # TODO DANIIL add your curl stuff for Kubernetes

        if mqttClient == None:
            continue
        # TODO create topic message for event
        mqttClient = pyMqtt.MQTTClient(url,"openGateClient")
        # TODO publish message
        # mqttClient.publish()
    mqttClient.disconnect()    
    return

# Author Andrew Pegg
def on_new_sample(appsink):
    """Callback function for receiving new detection results."""
    sample = appsink.emit("pull-sample")
    if sample:
        # Get the detection results as text or other data
        buffer = sample.get_buffer()
        
        # Extract all detection results for this frame
        detection_text = buffer.extract_dup(0, buffer.get_size()).decode('utf-8')
        
        print(f"All detections for this frame: {detection_text}")
        # submit sample to the muti-process queue
        sampleQueue.put_nowait(detection_text)
    return Gst.FlowReturn.OK

# Author Andrew Pegg
def construct_pipeline(pipe):
    """Initialize and link elements for the GStreamer pipeline."""
    # Parse arguments
    parser = argparse.ArgumentParser(
        add_help=False,
        formatter_class=type(
            "CustomFormatter",
            (
                argparse.ArgumentDefaultsHelpFormatter,
                argparse.RawTextHelpFormatter,
            ),
            {},
        ),
    )
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help=DESCRIPTION,
    )
    parser.add_argument(
        "--rtsp", type=str, default=DEFAULT_RTSP_SRC,
        help="RTSP URL"
    )
    parser.add_argument(
        "--detection_model", type=str, default=DEFAULT_DETECTION_MODEL,
        help="Path to TfLite Object Detection Model"
    )
    parser.add_argument(
        "--detection_module", type=str, default=DEFAULT_DETECTION_MODULE,
        help="Object Detection module for post-processing"
    )
    parser.add_argument(
        "--detection_labels", type=str, default=DEFAULT_DETECTION_LABELS,
        help="Path to TfLite Object Detection Labels"
    )
    parser.add_argument(
        "--detection_constants", type=str, default=DEFAULT_DETECTION_CONSTANTS,
        help="Constants for TfLite Object Detection Model"
    )

    args = parser.parse_args()

    detection = {
        "model": args.detection_model,
        "module": args.detection_module,
        "labels": args.detection_labels,
        "constants": args.detection_constants
    }

    # Check if all model and label files are present
    if not os.path.exists(detection["model"]):
        print(f"File {detection['model']} does not exist")
        sys.exit(1)
    if not os.path.exists(detection["labels"]):
        print(f"File {detection['labels']} does not exist")
        sys.exit(1)

    # Create all elements
    elements = {
        "rtspsrc":      create_element("rtspsrc", "rtspsrc"),
        "rtph264depay": create_element("rtph264depay", "rtph264depay"),
        "capsfilter_0": create_element("capsfilter", "rtph264depaycaps"),
        "h264parse":    create_element("h264parse", "h264parser"),
        "v4l2h264dec":  create_element("v4l2h264dec", "v4l2h264decoder"),
        "deccaps":      create_element("capsfilter", "deccaps"),
        "mlvconverter": create_element("qtimlvconverter", "converter"), # decode video into tensors for mltflite
        "queue_0":      create_element("queue","queue0"),
        "mltflite":     create_element("qtimltflite", "inference"),
        "queue_1":      create_element("queue","queue1"),
        "mlvdetection": create_element("qtimlvdetection", "detection"),
        "textcapsfilter": create_element("capsfilter","textCapsFilter"),
        "queue_2":        create_element("queue","queue2"),
        "appsink":      create_element("appsink", "appsink")  # appsink for result collection
    }

    # Set element properties
    Gst.util_set_object_arg(elements["rtspsrc"], "location", args.rtsp)

    Gst.util_set_object_arg(
        elements["capsfilter_0"],
        "caps",
        "video/x-h264,colorimetry=bt709",
    )

    Gst.util_set_object_arg(elements["h264parse"], "config-interval", "1")

    Gst.util_set_object_arg(elements["v4l2h264dec"], "capture-io-mode", "dmabuf")
    Gst.util_set_object_arg(elements["v4l2h264dec"], "output-io-mode", "dmabuf")

    Gst.util_set_object_arg(
        elements["deccaps"], "caps", "video/x-raw,format=NV12"
    )

    Gst.util_set_object_arg(elements["mltflite"], "delegate", "external")
    Gst.util_set_object_arg(
        elements["mltflite"],
        "external-delegate-path",
        "libQnnTFLiteDelegate.so",
    )
    Gst.util_set_object_arg(
        elements["mltflite"],
        "external-delegate-options",
        "QNNExternalDelegate,backend_type=htp,htp_precision=(string)1;",
    )
    Gst.util_set_object_arg(elements["mltflite"], "model", detection["model"])

    Gst.util_set_object_arg(elements["mlvdetection"], "threshold", "75.0")
    Gst.util_set_object_arg(elements["mlvdetection"], "results", "4")
    Gst.util_set_object_arg(
        elements["mlvdetection"], "module", detection["module"]
    )
    Gst.util_set_object_arg(
        elements["mlvdetection"], "constants", detection["constants"],
    )
    Gst.util_set_object_arg(
        elements["mlvdetection"], "labels", detection["labels"]
    )

    Gst.util_set_object_arg(elements['textcapsfilter'],'caps','text/x-raw')

    Gst.util_set_object_arg(elements["appsink"], "emit-signals", True)
    elements["appsink"].connect("new-sample", on_new_sample)

    # Add all elements
    for element in elements.values():
        pipe.add(element)

    # Link all elements
    link_orders = [
        [
            "rtph264depay", "capsfilter_0", "h264parse", "v4l2h264dec", "deccaps", "mlvconverter",
            "queue_0","mltflite", "queue_1" ,"mlvdetection",'textcapsfilter', "queue_2" , "appsink"
        ]
    ]
    link_elements(link_orders, elements)

    def on_pad_added(elem, pad, dest):
        if "rtp" in pad.get_name():
            sink_pad = dest.get_static_pad("sink")
            if (
                not sink_pad.is_linked()
                and pad.link(sink_pad) != Gst.PadLinkReturn.OK
            ):
                raise (
                    f"Failed to link {elem.get_name()} with {dest.get_name()}!"
                )

    elements["rtspsrc"].connect(
        "pad-added", on_pad_added, elements["rtph264depay"]
    )

# Author Qualcomm
def quit_mainloop(loop):
    """Quit the mainloop if it is running."""
    if loop.is_running():
        print("Quitting mainloop!")
        loop.quit()
    else:
        print("Loop is not running!")

# Author QualComm
def bus_call(_, message, loop):
    """Handle bus messages."""
    global eos_received

    message_type = message.type
    if message_type == Gst.MessageType.EOS:
        print("EoS received!")
        eos_received = True
        quit_mainloop(loop)
    elif message_type == Gst.MessageType.ERROR:
        error, debug_info = message.parse_error()
        print("ERROR:", message.src.get_name(), " ", error.message)
        if debug_info:
            print("debugging info:", debug_info)
        quit_mainloop(loop)
    return True


# Author Qualcomm
def handle_interrupt_signal(pipe, loop):
    """Handle ctrl+C signal."""
    _, state, _ = pipe.get_state(Gst.CLOCK_TIME_NONE)
    if state == Gst.State.PLAYING:
        event = Gst.Event.new_eos()
        if pipe.send_event(event):
            print("EoS sent!")
        else:
            print("Failed to send EoS event to the pipeline!")
            quit_mainloop(loop)
    else:
        print("Pipeline is not playing, terminating!")
        quit_mainloop(loop)
    return GLib.SOURCE_CONTINUE

# Author Andrew Pegg
def start_worker(queue:multiprocessing.Queue):
    worker_process = multiprocessing.Process(target=processSample,args=(queue,)) # pass blocking queue to sub process
    worker_process.daemon = True
    worker_process.start() # start but dont wait on execution
    return worker_process

# Author QualComm
def is_linux():
    try:
        with open("/etc/os-release") as f:
            for line in f:
                if "Linux" in line:
                    return True
    except FileNotFoundError:
        return False
    return False

def main():
    """Main function to set up and run the GStreamer pipeline."""

    # Set the environment (leave for now test with it removed and see if anything breaks)
    if is_linux():
        os.environ["XDG_RUNTIME_DIR"] = "/dev/socket/weston"
        os.environ["WAYLAND_DISPLAY"] = "wayland-1"

    Gst.init(None)
    worker = start_worker(sampleQueue)
    try:
        pipe = Gst.Pipeline()
        if not pipe:
            raise Exception("Failed to create pipeline!")
        construct_pipeline(pipe)
    except Exception as e:
        print(f"{e}")
        Gst.deinit()
        return 1

    loop = GLib.MainLoop()

    bus = pipe.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    interrupt_watch_id = GLib.unix_signal_add(
        GLib.PRIORITY_HIGH, signal.SIGINT, handle_interrupt_signal, pipe, loop
    )

    pipe.set_state(Gst.State.PLAYING)
    loop.run()

    GLib.source_remove(interrupt_watch_id)
    bus.remove_signal_watch()
    bus = None

    pipe.set_state(Gst.State.NULL)
    loop = None
    pipe = None

    Gst.deinit()
    worker.terminate() # stop worker process
    if eos_received:
        print("AI detection closed")


    return 0


if __name__ == "__main__":
    sys.exit(main())
