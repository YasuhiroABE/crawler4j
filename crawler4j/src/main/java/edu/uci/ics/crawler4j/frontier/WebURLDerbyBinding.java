package edu.uci.ics.crawler4j.frontier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.uci.ics.crawler4j.url.WebURL;

/**
 * Derby binding for WebURL to replace Berkeley DB TupleBinding
 */
public class WebURLDerbyBinding {

    public WebURL entryToObject(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            WebURL webURL = new WebURL();
            webURL.setURL(dis.readUTF());
            webURL.setDocid(dis.readInt());
            webURL.setParentDocid(dis.readInt());
            webURL.setParentUrl(dis.readUTF());
            webURL.setDepth(dis.readShort());
            webURL.setPriority(dis.readByte());
            webURL.setAnchor(dis.readUTF());
            return webURL;

        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize WebURL", e);
        }
    }

    public byte[] objectToEntry(WebURL url) {
        if (url == null) {
            return new byte[0];
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeUTF(url.getURL() != null ? url.getURL() : "");
            dos.writeInt(url.getDocid());
            dos.writeInt(url.getParentDocid());
            dos.writeUTF(url.getParentUrl() != null ? url.getParentUrl() : "");
            dos.writeShort(url.getDepth());
            dos.writeByte(url.getPriority());
            dos.writeUTF(url.getAnchor() != null ? url.getAnchor() : "");

            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize WebURL", e);
        }
    }
}
