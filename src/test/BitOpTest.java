package test;

public class BitOpTest {
    public static void main(String[] args) {
        byte b = 1;
        printBits(b);
        int count=0;
        for(int i = 7; i >= 0; --i) {
            int var1 = b & 255;
            int var2=1<<i;
            int var3= var1&var2;
            printBits(var1);
            printBits(var2);
            printBits(var3);
            boolean a = (b & 255 & 1 << i) == 0;
            if (!a) {
                break;
            }

            ++count;
        }
        System.out.println(" ");
    }
    public static void printBits(byte b) {
        for (int i = 7; i >= 0; i--) { // 从高位（第7位）到低位（第0位）
            int bit = (b >> i) & 1;   // 右移i位后，取最低位
            System.out.print(bit);    // 打印每一位
        }
        System.out.println();         // 换行
    }
    public static void printBits(int num) {
        int maskedNum = num & 0xFF; // 取最低的8位
        for (int i = 7; i >= 0; i--) { // 从第7位到第0位逐位打印
            int bit = (maskedNum >> i) & 1; // 右移i位后取最低位
            System.out.print(bit);
        }
        System.out.println(); // 换行
    }
}
