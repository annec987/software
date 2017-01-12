package com.cg.syscab.integration;

public class SyncServiceResult {
    private int total;
    private int update;
    private int create;
    private int delete;
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public int getUpdate() {
		return update;
	}
	public void setUpdate(int update) {
		this.update = update;
	}
	public int getCreate() {
		return create;
	}
	public void setCreate(int create) {
		this.create = create;
	}
	public int getDelete() {
		return delete;
	}
	public void setDelete(int delete) {
		this.delete = delete;
	}
	public String toLogString(){
		return String.format(" (total:%s) (create:%s) (update:%s) (delete:%s)", this.total , this.create , this.update , this.delete);
	}
}
