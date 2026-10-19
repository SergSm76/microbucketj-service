package my.app.microbucketj.lock;

import my.app.microbucketj.api.model.LockEntry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockRegistryTest {

  @Test
  void acquireWhenAbsent_applied() {
    LockRegistry r = new LockRegistry();
    var res = r.acquire("job-1", now() + 10_000, "node-A");
    assertTrue(res.applied());
    assertEquals("node-A", res.current().lockedBy());
  }

  @Test
  void acquireWhenHeldByOther_rejected() {
    LockRegistry r = new LockRegistry();
    r.acquire("job-1", now() + 60_000, "node-A");
    var res = r.acquire("job-1", now() + 10_000, "node-B");
    assertFalse(res.applied());
    assertEquals("node-A", res.current().lockedBy());
  }

  @Test
  void acquireWhenExpired_takenOver() {
    AtomicLong clock = new AtomicLong(1_000_000L);
    LockRegistry r = new LockRegistry(clock::get);
    r.acquire("job-1", 2_000_000L, "node-A");  // lock_until = 2_000_000
    clock.set(3_000_000L);                      // истекла
    var res = r.acquire("job-1", 4_000_000L, "node-B");
    assertTrue(res.applied());
    assertEquals("node-B", res.current().lockedBy());
  }

  @Test
  void updateByOwner_extends() {
    LockRegistry r = new LockRegistry();
    r.acquire("job-1", now() + 10_000, "node-A");
    var res = r.update("job-1", now() + 60_000, "node-A");
    assertTrue(res.applied());
    assertTrue(res.current().lockUntilMs() > now() + 10_000);
  }

  @Test
  void updateByOther_rejected() {
    LockRegistry r = new LockRegistry();
    r.acquire("job-1", now() + 60_000, "node-A");
    var res = r.update("job-1", now() + 60_000, "node-B");
    assertFalse(res.applied());
  }

  @Test
  void updateWhenAbsent_notApplied() {
    LockRegistry r = new LockRegistry();
    var res = r.update("job-1", now() + 10_000, "node-A");
    assertFalse(res.applied());
    assertNull(res.current());
  }

  @Test
  void releaseByOwner_applied() {
    LockRegistry r = new LockRegistry();
    r.acquire("job-1", now() + 60_000, "node-A");
    var res = r.release("job-1", "node-A");
    assertTrue(res.applied());
    assertNull(r.get("job-1"));
  }

  @Test
  void releaseByOther_rejected() {
    LockRegistry r = new LockRegistry();
    r.acquire("job-1", now() + 60_000, "node-A");
    var res = r.release("job-1", "node-B");
    assertFalse(res.applied());
    assertNotNull(r.get("job-1"));
  }

  @Test
  void delete_removesEntry() {
    LockRegistry r = new LockRegistry();
    r.acquire("job-1", now() + 60_000, "node-A");
    assertTrue(r.delete("job-1"));
    assertFalse(r.delete("job-1"));
  }

  @Test
  void cleanupExpired_removesOnlyExpired() {
    AtomicLong clock = new AtomicLong(1_000_000L);
    LockRegistry r = new LockRegistry(clock::get);
    r.acquire("live",   2_000_000L, "node-A");
    r.acquire("dying",  1_500_000L, "node-A");
    clock.set(1_600_000L);
    int removed = r.cleanupExpired();
    assertEquals(1, removed);
    assertNotNull(r.get("live"));
    assertNull(r.get("dying"));
  }

  @Test
  void applyRemote_lwwByLockedAt() {
    LockRegistry r = new LockRegistry();
    r.applyRemote(new LockEntry("job-1", 100_000L, 1_000L, "A"));
    r.applyRemote(new LockEntry("job-1", 200_000L, 2_000L, "B"));
    assertEquals("B", r.get("job-1").lockedBy());
    assertEquals(200_000L, r.get("job-1").lockUntilMs());
  }

  @Test
  void applyRemote_staleIgnored() {
    LockRegistry r = new LockRegistry();
    r.applyRemote(new LockEntry("job-1", 100_000L, 2_000L, "A"));
    r.applyRemote(new LockEntry("job-1", 99_999L,  1_000L, "B"));  // меньше lockedAt
    assertEquals("A", r.get("job-1").lockedBy());
  }

  private static long now() { return System.currentTimeMillis(); }
}
