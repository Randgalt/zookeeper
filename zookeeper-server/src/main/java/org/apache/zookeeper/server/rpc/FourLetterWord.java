package org.apache.zookeeper.server.rpc;

import org.apache.jute.rpc.JuteRpc;

@SuppressWarnings("unused")
class FourLetterWord {
    @JuteRpc(15)
    native String fourLetter(String word);

    @JuteRpc(16)
    native String fourLetterSetTraceMask(long traceMask);
}
