package com.zhongsou.spider.exception;

public class EmptyParamValueException extends Exception {

	@Override
	public String getMessage() {
		// TODO Auto-generated method stub
		return "param value should allow to be null";
	}

}
