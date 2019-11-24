package org.qbroker.common;

/* SerialPortDevice.java - a connector for synchronous I/O via a serial port */

import javax.comm.SerialPort;
import javax.comm.CommPortIdentifier;
import javax.comm.PortInUseException;
import javax.comm.UnsupportedCommOperationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Enumeration;
import java.util.TooManyListenersException;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Event;

/**
 * SerialPortDevice opens a serial port for synchronous I/O of a byte stream.
 * It does not perform any I/O.  It is User's job to read/write the bytes
 * from/to the stream.
 *<br><br>
 * @see #getInputStream
 * @see #getOutputStream
 *<br>
 * @author yannanlu@yahoo.com
 */


public class SerialPortDevice {
    private String portName;
    private int baudRate = 9600;
    private int dataBits = SerialPort.DATABITS_8;
    private int stopBits = SerialPort.STOPBITS_1;
    private int parity = SerialPort.PARITY_NONE;
    private int flowControlMode = SerialPort.FLOWCONTROL_NONE;
    private int timeout = 0;
    private SerialPort serialPort = null;
    private CommPortIdentifier portId = null;
    private InputStream in = null;
    private OutputStream out = null;
    private String operation = null;

    /** creates new SerialPortDevice
     * @param portName the device name or path to the Serial Port
     * @param baudRate the Baud Rate of the Serial Port
     * @param dataBits the number of data bits
     * @param stopBits string for the number of stop bits
     * @param parity   parity string
     * @param operation  either "read", "write" or "read-write (default)"
     * @param timeout  receive timeout in ms
     */
    public SerialPortDevice(String portName, int baudRate, int dataBits,
        String stopBits, String parity, String flowControl, String operation,
        int timeout) throws java.io.IOException {

        this.portName = portName;
        this.baudRate = baudRate;

        if (operation != null)
            this.operation = operation.toLowerCase();
        else
            this.operation = "read-write";
        switch (dataBits) {
          case 5:
            this.dataBits = SerialPort.DATABITS_5;
            break;
          case 6:
            this.dataBits = SerialPort.DATABITS_6;
            break;
          case 7:
            this.dataBits = SerialPort.DATABITS_7;
            break;
          case 8:
            this.dataBits = SerialPort.DATABITS_8;
            break;
          default:
            this.dataBits = SerialPort.DATABITS_8;
            break;
        }

        if (stopBits.equals("1")) {
            this.stopBits = SerialPort.STOPBITS_1;
        }
        else if (stopBits.equals("1.5")) {
            this.stopBits = SerialPort.STOPBITS_1_5;
        }
        else if (stopBits.equals("2")) {
            this.stopBits = SerialPort.STOPBITS_2;
        }
        else {
            this.stopBits = SerialPort.STOPBITS_1;
        }

        if (parity.equals("None")) {
            this.parity = SerialPort.PARITY_NONE;
        }
        else if (parity.equals("Even")) {
            this.parity = SerialPort.PARITY_EVEN;
        }
        else if (parity.equals("Odd")) {
            this.parity = SerialPort.PARITY_ODD;
        }
        else {
            this.parity = SerialPort.PARITY_NONE;
        }

        if (flowControl.equals("None")) {
            flowControlMode = SerialPort.FLOWCONTROL_NONE;
        }
        else if (flowControl.equals("RTS/CTS")) {
            flowControlMode = ("read".equals(operation)) ?
                SerialPort.FLOWCONTROL_RTSCTS_IN :
                SerialPort.FLOWCONTROL_RTSCTS_OUT;
        }
        else if (flowControl.equals("XON/XOFF")) {
            flowControlMode = ("read".equals(operation)) ?
                SerialPort.FLOWCONTROL_XONXOFF_IN :
                SerialPort.FLOWCONTROL_XONXOFF_OUT;
        }
        else {
            flowControlMode = SerialPort.FLOWCONTROL_NONE;
        }

        if (timeout >= 0 && !"write".equals(operation))
            this.timeout = timeout;

        portInit();
    }

    /**
     * initializes the serial port
     */
    private void portInit() throws IOException {
        Enumeration portList;
        boolean portFound = false;

        portList = CommPortIdentifier.getPortIdentifiers();

        while (portList.hasMoreElements()) {
            portId = (CommPortIdentifier) portList.nextElement();

            if (portId.getPortType() != CommPortIdentifier.PORT_SERIAL)
                continue;
            if (portId.getName().equals(this.portName)) {
                GenericLogger.log(Event.INFO, "Found port " + this.portName);
                portFound = true;
                break;
            }
        }
        if (!portFound) {
            throw(new IOException("port " + this.portName + " not found"));
        }
        portOpen();
    }

    /**
     * opens the serial port
     */
    public void portOpen() throws IOException {
        try {
            serialPort = (SerialPort) portId.open("SerialPortDevice", 5000);
        }
        catch (PortInUseException e) {
            throw(new IOException("port " + this.portName + " in use"));
        }

        if (timeout > 0) try {
            serialPort.enableReceiveTimeout(timeout);
        }
        catch (Exception e) {
        }

        if (serialPort.isReceiveTimeoutEnabled())
            GenericLogger.log(Event.DEBUG, "Enabled timeout on " +this.portName+
                ": "+ serialPort.getReceiveTimeout());

        if ("read".equals(operation)) {
            in = serialPort.getInputStream();
            serialPort.notifyOnDataAvailable(true);
        }
        else if ("write".equals(operation)) {
            out = serialPort.getOutputStream();
            serialPort.notifyOnOutputEmpty(true);
        }
        else { // for read and write
            in = serialPort.getInputStream();
            serialPort.notifyOnDataAvailable(true);
            out = serialPort.getOutputStream();
            serialPort.notifyOnOutputEmpty(true);
        }

        try {
            serialPort.setSerialPortParams(this.baudRate,
                this.dataBits, this.stopBits, this.parity);
            if (flowControlMode != SerialPort.FLOWCONTROL_NONE)
                serialPort.setFlowControlMode(flowControlMode);
        }
        catch (UnsupportedCommOperationException e) {
            throw(new IOException("Unsuppored parameters on " + this.portName));
        }
/*
        int i = serialPort.getFlowControlMode();
        switch (i) {
          case SerialPort.FLOWCONTROL_XONXOFF_IN:
            System.out.println("XONXOFF_IN: " + i);
            break;
          case SerialPort.FLOWCONTROL_XONXOFF_OUT:
            System.out.println("XONXOFF_OUT: " + i);
            break;
          case SerialPort.FLOWCONTROL_RTSCTS_IN:
            System.out.println("RTSCTS_IN: " + i);
            break;
          case SerialPort.FLOWCONTROL_RTSCTS_OUT:
            System.out.println("RTSCTS_OUT: " + i);
            break;
          case SerialPort.FLOWCONTROL_NONE:
            System.out.println("NONE: " + i);
            break;
          default:
            System.out.println("UNKNOWN: " + i);
            break;
        }
*/
    }

    /**
     * @return <code>OutputStream</code> if operation is to write
     *         <code>null</code> otherwise
     */
    public OutputStream getOutputStream() {
        return out;
    }

    /**
     * @return <code>InputStream</code> if operation is to read
     *         <code>null</code> otherwise
     */
    public InputStream getInputStream() {
        return in;
    }

    public String getPortName() {
        return portName;
    }

    public String getOperation() {
        return operation;
    }

    public void close() throws IOException {
        if (serialPort != null) {
            serialPort.close();
        }
    }
}
