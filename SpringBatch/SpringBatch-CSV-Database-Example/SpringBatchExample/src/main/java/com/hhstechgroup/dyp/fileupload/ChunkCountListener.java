package com.hhstechgroup.dyp.fileupload;
import java.text.MessageFormat;
import java.time.LocalDateTime;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

/**
 * Log the count of items processed at a specified interval.
 * 
 * @author Jeremy Yearron
 *
 */
public class ChunkCountListener implements ChunkListener{
	

	private MessageFormat fmt = new MessageFormat("{0} items processed");

	private int loggingInterval = 1000;
		
	@Override
	public void beforeChunk(ChunkContext context) {
		// Nothing to do here
	}

	@Override
	public void afterChunk(ChunkContext context) {
		
		int count = context.getStepContext().getStepExecution().getReadCount();
		
		// If the number of records processed so far is a multiple of the logging interval then output a log message.			
		if (count > 0 && count % loggingInterval == 0) {
			System.out.println(count);
		}
	}
	
	@Override
	public void afterChunkError(ChunkContext context) {
		// Nothing to do here		
	}
	
	public void setItemName(String itemName) {
		this.fmt = new MessageFormat("{0} " + itemName + " processed");
	}

	public void setLoggingInterval(int loggingInterval) {
		this.loggingInterval = loggingInterval;
	}
}