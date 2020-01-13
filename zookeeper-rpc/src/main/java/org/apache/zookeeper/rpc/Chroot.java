package org.apache.zookeeper.rpc;

class Chroot {
    private final String chrootPath;

    Chroot(String chrootPath) {
        this.chrootPath = (chrootPath != null) ? chrootPath : "";
    }

    String fixPath(String path) {
        return chrootPath.isEmpty() ? path : (chrootPath + path);
    }

    String unFixPath(String path) {
        return chrootPath.isEmpty() ? path : path.substring(chrootPath.length());
    }
}
