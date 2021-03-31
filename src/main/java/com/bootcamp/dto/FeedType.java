package com.bootcamp.dto;

public enum FeedType {
  ABSOLUTE("ABSOLUTE"),
  DELTA("DELTA");

  private String displayName;

  FeedType(final String displayName) {
    this.displayName = displayName;
  }

  public String getDisplayName() {
    return displayName;
  }
}
