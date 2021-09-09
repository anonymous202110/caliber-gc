package edu.msu.cse.causalSpartan.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import com.google.protobuf.ByteString;
import java.io.File;  // Import the File class

import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.metadata.Metadata.DSVMessage;
import edu.msu.cse.dkvf.metadata.Metadata.Record;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;
import edu.msu.cse.dkvf.metadata.Metadata.VVMessage;
import java.util.logging.Logger;
import edu.msu.cse.dkvf.Utils;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;

public class DsvComputation implements Runnable {

	CausalSpartanServer server;
	Long mgvt;
	Long dMaxUt;
	int dMaxSr;
	Logger LOGGER;
	Integer counter;
	public DsvComputation(CausalSpartanServer server) {
		this.server = server;
		counter = 0;
		dMaxUt = new Long(0);
		dMaxSr = 0;
	}

	Predicate<Record> dMaxPredicate = (Record r) -> {
		if(r.getUt() <= mgvt.longValue())
		{
			return true;
		}
		else
			return false;

	};// this predicate operated on 10 records and in the end we got true for 5 records

	private void getDMax(List<Record> records) { // this private function operated on 5 records and chose 1 as dmax record
		for(Record record : records) {
			if(dMaxUt.longValue() == record.getUt()) {
				dMaxSr = Math.max(dMaxSr,record.getSr());
			}
			else if(dMaxUt < record.getUt()) {
				dMaxUt = new Long(record.getUt());
				dMaxSr = record.getSr();
			}
		}
	}

	Predicate<Record> isDeletable = (Record r) -> {
		byte[] deleteValue = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0".getBytes();
		// for(int i=0; i<16; i++)
		// 	deleteValue[i] = "\000".getBytes();
		ByteString nullValue = ByteString.copyFrom(deleteValue);
		// server.frameworkLOGGER.severe("nullValue is: " + nullValue.toStringUtf8() + " | " + r.getValue().toStringUtf8() + " | " + r );
		// server.frameworkLOGGER.severe("bytes size is: " + nullValue);
		if(r.getUt() < dMaxUt.longValue())
			return true;
		else if(r.getUt() == dMaxUt.longValue() && r.getSr() < dMaxSr)
			return true;
		else if(r.getUt() == dMaxUt && r.getSr() == dMaxSr &&  (r.getValue().toStringUtf8()).equals(nullValue.toStringUtf8()))
		{
			return true;
		}
		else
			return false;
	};// this final predicate operates on 10 records and it will return true for 4 records

	@Override
	public void run() {
		//take minimum of all childrens 
		List<Long> minVv = new ArrayList<Long>();
		for (AtomicLong v : server.vv) {
			minVv.add(v.get());
		}
		
		for (Map.Entry<Integer, List<Long>> childVv : server.childrenVvs.entrySet()) {
			for (int i = 0; i < childVv.getValue().size(); i++) {
				if (minVv.get(i) > childVv.getValue().get(i))
					minVv.set(i, childVv.getValue().get(i));
			}
		}

		//if the node is parent it send DsvMessage to its children
		ServerMessage sm = null;
		if (server.parentPId == server.pId) {
			server.setDsv(minVv);
			garbageCollect();
			sm = ServerMessage.newBuilder().setDsvMessage(DSVMessage.newBuilder().addAllDsvItem(minVv)).build();
			server.sendToAllChildren(sm);
		}
		//if the node is not root, it send vvMessage to its parent.
		else {
			VVMessage vvM = VVMessage.newBuilder().setPId(server.pId).addAllVvItem(minVv).build();
			sm = ServerMessage.newBuilder().setVvMessage(vvM).build();
			server.sendToServerViaChannel(server.dcId + "_" + server.parentPId, sm);
			garbageCollect();
		}
		counter++;
	}

	private void garbageCollect() {
		if(server.garbageCollect == 1) {
			List<Long> dsvCopy = new ArrayList<>();
			for(int i=0; i<server.dsv.size(); i++) {
				dsvCopy.add(i, server.dsv.get(i));
			}
			Long minimum = new Long(Long.MAX_VALUE);
			for(int i=0; i< dsvCopy.size(); i++) {
				minimum = Math.min(dsvCopy.get(i), minimum);
			}
			synchronized(server.gvt) {
				server.gvt.set(server.dcId,minimum);
			}

			mgvt= new Long(Long.MAX_VALUE);

			for(int i=0; i< server.gvt.size();i++) {
				mgvt= Math.min(server.gvt.get(i), mgvt);
			}

			/** delete **/
			List<String> keys = new ArrayList<>();
			try {
				if(server.getKeys(keys) == StorageStatus.SUCCESS) {
					for (String key : keys) {
						dMaxUt = new Long(0);
						List<Record> results = new ArrayList<>();
						server.readAll(key, dMaxPredicate, results);
						getDMax(results);
						StorageStatus ss = server.deleteVersion(key, isDeletable);
						if(ss == StorageStatus.SUCCESS)
							server.frameworkLOGGER.severe("Garbage Collection is successful");
						else
							server.frameworkLOGGER.severe("Error in deleting versions");
					}
				}
				else {
					server.frameworkLOGGER.severe("DB Failed to get keys");
				}
			} catch(Exception e) {
				server.frameworkLOGGER.severe("Exception in getting keys list" + e.toString());
			}
		}
	}
}