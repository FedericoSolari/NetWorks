from lib.constants import SELECTIVE_REPEAT
from protocol.selective_repeat import SelectiveRepeatProtocol
from protocol.stop_and_wait import StopAndWaitProtocol

def select_protocol(protocol):
    if protocol == SELECTIVE_REPEAT:
        return SelectiveRepeatProtocol
    else:
        return StopAndWaitProtocol