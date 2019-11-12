package com.zhongsou.robots;

public class BMTrieNode {
  private byte data;
  private BMTrieNode [] childs;
  private byte [] childsBitmap;

  public BMTrieNode() {
    data = ' ';
    childs = new BMTrieNode[256];
    childsBitmap = new byte[32];
  }

  public void setData(byte ch) {
    data = ch;
  }

  public byte getData() {
    return data;
  }
}
