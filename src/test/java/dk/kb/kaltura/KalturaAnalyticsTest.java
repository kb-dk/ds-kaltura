package dk.kb.kaltura;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.kaltura.client.types.APIException;
import com.kaltura.client.types.MediaEntry;
import dk.kb.kaltura.client.DsKalturaAnalytics;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KalturaAnalyticsTest {

  DsKalturaAnalytics dsKalturaAnalytics;

  @BeforeEach
  void beforeEach() throws APIException {
    dsKalturaAnalytics = new DsKalturaAnalytics(
        "https://example.com",
        "",
        0,
        "",
        "",
        "",
        600,
        0);
  }

  @Test
  public void getEntriesFromIdList_whenListOfObjectIdsIsNull_thenThrowIllegalArgumentException() {
    // Arrange
    List<String> nullList = null;

    // Act and Assert
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> dsKalturaAnalytics.getEntriesFromIdList(nullList));
    assertEquals("Null or empty objectIds list", exception.getMessage());
  }

  @Test
  public void getEntriesFromIdList_whenListOfObjectIdsIsEmpty_thenThrowIllegalArgumentException() {
    // Arrange
    List<String> emptyList = List.of();

    // Act and Assert
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> dsKalturaAnalytics.getEntriesFromIdList(emptyList));
    assertEquals("Null or empty objectIds list", exception.getMessage());
  }

  @Test
  public void listEntryBatch_whenListOfObjectIdsIsGreaterThanBatchSize_thenThrowIllegalArgumentException() {
    // Arrange
    List<String> mockList = mock(List.class);
    when(mockList.size()).thenReturn(501);

    // Act and Assert
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> dsKalturaAnalytics.listEntryBatch(mockList));
    assertEquals("Size of objectIds: 501 is greater than batchSize: 500", exception.getMessage());
  }

  @Test
  public void listEntryBatch_whenHavingOneObjectId_thenReturnCorrespondingMediaEntry()
      throws APIException {
    // Arrange
    DsKalturaAnalytics spyDsKalturaAnalytics = spy(dsKalturaAnalytics);

    UUID objectId = UUID.fromString("a00a0a00-a0aa-00a0-a000-00aaa00000a0");

    MediaEntry mediaEntry = new MediaEntry();
    mediaEntry.setReferenceId(objectId.toString());
    List<MediaEntry> mediaEntryList = List.of(mediaEntry);

    List<String> objectIds = List.of(objectId.toString());

    doReturn(mediaEntryList).when(spyDsKalturaAnalytics).getMediaEntries(objectIds);

    // Act
    List<MediaEntry> resultBaseEntryList = spyDsKalturaAnalytics.listEntryBatch(objectIds);

    // Assert
    assertEquals(1, resultBaseEntryList.size());
  }
}
