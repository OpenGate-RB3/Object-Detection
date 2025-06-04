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
import re

DESCRIPTION = """
The application receives an RTSP stream as source, decodes it, uses YOLOv8
TFLite model to identify the object in the scene from the camera stream and
returns all detections for each frame as a Python list.
"""


# Configurations for Detection (May need to be changed for each model configured)
DEFAULT_DETECTION_MODEL = "/etc/models/yolov8_det.tflite"
DEFAULT_DETECTION_MODULE = "yolov8"
DEFAULT_DETECTION_LABELS = "/etc/labels/coco_labels.txt"
DEFAULT_DETECTION_CONSTANTS = "YoloV8,q-offsets=<33.0,0.0,0.0>,\
    q-scales=<3.243085384,0.0037704327,1.0>;"


eos_received = False

sampleQueue = multiprocessing.Queue()
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
    # define parsing regex
    labels = [
    "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck", "boat",
    "traffic light", "fire hydrant", "stop sign", "parking meter", "bench", "bird", "cat",
    "dog", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack",
    "umbrella", "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball",
    "kite", "baseball bat", "baseball glove", "skateboard", "surfboard", "tennis racket",
    "bottle", "wine glass", "cup", "fork", "knife", "spoon", "bowl", "banana", "apple",
    "sandwich", "orange", "broccoli", "carrot", "hot dog", "pizza", "donut", "cake",
    "chair", "couch", "potted plant", "bed", "dining table", "toilet", "tv", "laptop",
    "mouse", "remote", "keyboard", "cell phone", "microwave", "oven", "toaster", "sink",
    "refrigerator", "book", "clock", "vase", "scissors", "teddy bear", "hair drier",
    "toothbrush"
    ]
    empty_pattern = r'bounding-boxes=\(int\)<\s*>'
    label_pattern = "|".join(re.escape(label) for label in sorted(labels, key=lambda x: -len(x)))
    label_regex  = rf'"({label_pattern}), id='
    confidence_regex = r'confidence=\(double\)([\d.]+)'
    while True:
        try:
            text_input = queue.get() # blocks until sample ready
            if text_input == None:
                break
            if not isinstance(text_input,str):
                continue
        except Exception as e:
            break
        text_input = text_input.replace('\\','')
        is_empty = bool(re.search(empty_pattern,text_input))
        if is_empty:
            continue
        found_labels  = re.findall(label_regex , text_input)
        found_confidences = re.findall(confidence_regex, text_input)
        label_conf_pairs = list(zip(found_labels, map(float, found_confidences)))
        if mqttClient == None:
            continue
        mqttClient.publish("detections", str(label_conf_pairs))
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

        # submit sample to the muti-process queue
        sampleQueue.put_nowait(detection_text)
    return Gst.FlowReturn.OK

# Author Andrew Pegg
def construct_pipeline():
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
    pipeline_str = """
qtiqmmfsrc name=qmmf !
video/x-raw,format=NV12,width=1920,height=1080,framerate=30/1 !
tee name=split
split. ! queue ! v4l2h264enc ! h264parse ! mpegtsmux ! udpsink host=192.168.0.44 port=5004
split. ! queue ! qtimlvconverter ! queue !
qtimltflite delegate=external external-delegate-path=libQnnTFLiteDelegate.so external-delegate-options="QNNExternalDelegate,backend_type=htp;" model=/etc/models/yolov8_det.tflite ! queue !
qtimlvdetection threshold=60.0 results=5 module=yolov8 labels=/etc/labels/coco_labels.txt constants="YoloV8,q-offsets=<33.0,0.0,0.0>,q-scales=<3.243085384,0.0037704327,1.0>;" !
text/x-raw ! queue !
appsink name=appsink emit-signals=true drop=true max-buffers=1 sync=false
    """
    pipeline = Gst.parse_launch(pipeline_str)
    appsink = pipeline.get_by_name("appsink")
    appsink.connect("new-sample", on_new_sample)
    return pipeline


  
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
    # if is_linux():
    os.environ["XDG_RUNTIME_DIR"] = "/dev/socket/weston"
    os.environ["WAYLAND_DISPLAY"] = "wayland-1"

    Gst.init(sys.argv)
    print("starting worker process")
    worker = start_worker(sampleQueue)
    print('worker process running')
    try:
        pipe = construct_pipeline()
        if not pipe:
            raise Exception("Failed to create pipeline!")     
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
    sampleQueue.put(None)
    sampleQueue.close()
    Gst.deinit()
    worker.join() # stop worker process
    if eos_received:
        print("AI detection closed")


    return 0


if __name__ == "__main__":
    sys.exit(main())
