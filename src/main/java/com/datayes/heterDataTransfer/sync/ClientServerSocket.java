package com.datayes.heterDataTransfer.sync;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Vector;

import static java.lang.System.out;


public class ClientServerSocket {
    //from EECS285
    private String ipAddr;
    private int portNum;
    private Socket socket;
    private DataInputStream inData;
    private DataOutputStream outData;

    public ClientServerSocket(String inIPAddr, int inPortNum) {
        socket = null;
        inData = null;
        outData = null;
        ipAddr = inIPAddr;
        portNum = inPortNum;
    }

    public ClientServerSocket(Socket _socket) {
        socket = _socket;
        try {
            inData = new DataInputStream(socket.getInputStream());
            outData = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startClient() {
        try {
            socket = new Socket(ipAddr, portNum);
            inData = new DataInputStream(socket.getInputStream());
            outData = new DataOutputStream(socket.getOutputStream());
        } catch (IOException ioe) {
            out.println("Starting a client failed");
            ioe.printStackTrace();
            System.exit(10);
        }
    }

    public boolean sendString(String strToSend) throws IOException {
        boolean success = false;
        try {
            outData.write(strToSend.getBytes()); // For emoji to work
            outData.writeByte(0);
            success = true;
        } catch (IOException ioe) {
            throw ioe;
        }

        return success;
    }

    public String recvString() throws IOException {
        Vector<Byte> byteVec = new Vector<Byte>();
        byte [] byteAry;
        byte recvByte;
        String receivedString = "";

        try {
            recvByte = inData.readByte();
            while (!(recvByte == 0 || recvByte == 10)) {
                byteVec.add(recvByte);
                recvByte = inData.readByte();
            }

            byteAry = new byte[byteVec.size()];
            for (int i = 0; i < byteVec.size(); ++ i) {
                byteAry[i] = byteVec.elementAt(i).byteValue();
            }
            receivedString = new String(byteAry, "UTF-8");
        } catch (IOException ioe) {
            out.println("Receive string failed");
            throw ioe;
        }
        return receivedString;
    }

    public void recvFile(String filename) throws IOException {
        out.println("Start receiving file "+filename);
        String fileLength = recvString();
        int byteNum = (int)Integer.parseInt(fileLength);
        Vector<Byte> byteBec = new Vector<Byte>();
        byte[] byteAry = new byte[byteNum];
        byte recvByte;
        try{
            FileOutputStream fos = new FileOutputStream(filename);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            int bytesRead = 0;
            bytesRead = inData.read(byteAry, 0, byteNum);
            out.println(bytesRead);
            int current = bytesRead;
            while(bytesRead > -1 & current < byteNum){
                bytesRead = inData.read(byteAry, current,
                        byteAry.length - current);
                out.println(bytesRead);
                if (bytesRead >= 0) current += bytesRead;
            }

            bos.write(byteAry, 0, current);
            bos.flush();
            System.out.println("File saved.");
            if (fos != null) fos.close();
            if (bos != null) bos.close();
        } catch (IOException ioe) {
            out.println("Receive file failed");
            throw ioe;
        }
    }

    public void sendFile(String fileToSend) throws IOException {
        try{
            File myFile = new File(fileToSend);
            System.out.println(myFile.length());
            byte [] byteAry = new byte[(int)myFile.length()];
            sendString(Integer.toString((int)myFile.length()));
            FileInputStream fis = new FileInputStream(myFile);
            BufferedInputStream bis = new BufferedInputStream(fis);
            bis.read(byteAry, 0, byteAry.length);
            outData.write(byteAry, 0, byteAry.length);
            outData.flush();
            System.out.println("Done.");
            fis.close();
            bis.close();
        } catch (IOException ioe) {
            out.println("Send string failed");
            throw ioe;
        }
    }

    public void sendByteAry(byte[] byteAry) throws IOException {
        outData.write(byteAry);
    }

    public byte[] recvByteAry(int numBytes) throws IOException {
        byte[] byteAry = new byte[numBytes];
        int bytesRead = 0, current = 0;
        while (bytesRead > -1 & numBytes > current){
            bytesRead = inData.read(byteAry, current, numBytes - current);
            if (bytesRead >= 0) current += bytesRead;
        }
        return byteAry;
    }

    public void sendBitSet(BitSet bitSet) throws IOException {
        sendInt(bitSet.size());
        sendInt(bitSet.toByteArray().length);
        outData.write(bitSet.toByteArray());
    }

    public BitSet recvBitSet() throws IOException {
        int setSize = recvInt();
        int numBytes = recvInt();
        BitSet ret = BitSet.valueOf(recvByteAry(numBytes));
        return ret;
    }

    public void sendLong(long l) throws IOException {
        outData.writeLong(l);
    }

    public long recvLong() throws IOException {
        byte[] byteAry = recvByteAry(8);
        ByteBuffer wrapped = ByteBuffer.wrap(byteAry);
        return wrapped.getLong();
    }

    public void sendInt(int i) throws IOException {
        outData.writeInt(i);
    }

    public int recvInt() throws IOException {
        byte[] byteAry = recvByteAry(4);
        ByteBuffer wrapped = ByteBuffer.wrap(byteAry);
        return wrapped.getInt();
    }
}

