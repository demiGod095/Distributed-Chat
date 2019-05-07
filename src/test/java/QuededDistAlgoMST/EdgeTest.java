package QuededDistAlgoMST;
import org.junit.Test;

import activitystreamer.server.ServerEdge;

import static org.junit.Assert.*;

import java.util.UUID;


public class EdgeTest {
    @Test public void testEdgeOrdering() {
    	UUID uuid1 = UUID.fromString("222a3222-3d80-4c8e-a6c2-eb51bbe0c33b");
    	UUID uuid2 = UUID.fromString("222a3222-3d80-4c8e-a6c2-eb51bbe0c33e");
    	UUID uuid3 = UUID.fromString("222a3222-3d80-4c8e-a6c2-eb51bbe0c33d");
    	ServerEdge edge1 = new ServerEdge(uuid1, uuid2, 2000);
    	ServerEdge edge2 = new ServerEdge(uuid1, uuid2, 1000);
    	ServerEdge edge3 = new ServerEdge(uuid1, uuid3, 2000);

    	assertTrue(edge2.compareTo(edge1) < 0);
    	assertFalse(edge1.compareTo(edge3) == 0);
    }
	
}
