package riscvstorage;
import java.nio.ByteBuffer;
public class Native {
    public static native long riscv_create_reader(String path, String fieldsStr);
    public static native int riscv_next_batch(long riscvReaderPtr, ByteBuffer buffer);
}