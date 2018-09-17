package com.ctrip.xpipe.redis.console.query;

import com.ctrip.xpipe.redis.console.exception.DalInsertException;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

/**
 * @author shyin
 *
 * Aug 26, 2016
 */
public class DalQueryHandler {
	public <T> T handleQuery(DalQuery<T> query) {
		try {
			return query.doQuery();
		} catch(DalException e) {
			if(e instanceof DalNotFoundException) {
				return null;
			}
			throw new ServerException("Load data failed." + e.getMessage(), e);
		}
	}

	public void handleUpdate(DalQuery<Integer> query) {
		try {
			Integer result = query.doQuery();
			if(result == null || result == 0) {
				throw new DalUpdateException("No rows updated");
			}
		} catch (Exception e) {
			throw new DalUpdateException("Update failed." + e.getMessage(), e);
		}
	}

	public void handleBatchUpdate(DalQuery<int[]> query) {
		try {
			int[] result = query.doQuery();
			if(result == null) {
				throw new DalUpdateException("No rows updated");
			}
			int changeSum = 0;
			for(int changedRows: result) {
				changeSum += changedRows;
			}
			if(changeSum == 0) {
				throw new DalUpdateException("No rows updated");
			}
		} catch (Exception e) {
			throw new DalUpdateException("Update failed." + e.getMessage(), e);
		}
	}

	public void handleDelete(DalQuery<Integer> query, boolean failAsSuccess) {
		try {
			Integer result = query.doQuery();
			if(!failAsSuccess) {
				if (result == null || result == 0) {
					throw new DalException("No row deleted");
				}
			}
		} catch (Exception e) {
			throw new DalUpdateException("Update failed." + e.getMessage(), e);
		}
	}

	public void handleBatchDelete(DalQuery<int[]> query, boolean failAsSuccess) {
		try {
			int[] result = query.doQuery();
			if(failAsSuccess) {
				return;
			}

			if(result == null) {
				throw new DalUpdateException("No rows updated");
			}
			int changeSum = 0;
			for(int changedRows: result) {
				changeSum += changedRows;
			}
			if(changeSum == 0) {
				throw new DalUpdateException("No rows updated");
			}
		} catch (Exception e) {
			throw new DalUpdateException("Update failed." + e.getMessage(), e);
		}
	}

	public void handleInsert(DalQuery<Integer> query) {
		try {
			Integer result = query.doQuery();
			if(result == null || result == 0) {
				throw new DalInsertException("No rows insert");
			}
		} catch (Exception e) {
			throw new DalInsertException("Insert failed." + e.getMessage(), e);
		}
	}

	public int[] handleBatchInsert(DalQuery<int[]> query) {
		try {
			int[] result = query.doQuery();

			if(result == null) {
				throw new DalInsertException("No rows insert");
			}
			int changeSum = 0;
			for(int changedRows: result) {
				changeSum += changedRows;
			}
			if(changeSum == 0) {
				throw new DalInsertException("No rows insert");
			}
			return result;
		} catch (Exception e) {
			throw new DalInsertException("Insert failed." + e.getMessage(), e);
		}
	}
}
