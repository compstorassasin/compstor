package riscvstorage;


public class RISCVLib {
    private static class Holder {
        static RISCVLib instance = new RISCVLib();
    }

    public static RISCVLib get() {
        return Holder.instance;
    }

    private RISCVLib() {
        System.loadLibrary("riscv");
    }
}
