package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import java.util.*;
/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement
/*We suggest breaking up the logic of this method into two phases: ensuring that we have the
 appropriate locks on ancestors, and acquiring the lock on the resource. You will need to promote in some cases, and escalate in some cases (these cases are not mutually exclusive).
  */

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) {
            return;
        }
        if (lockType == LockType.S) {
            if (lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.S) {
                return;
            }
            if (lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.IS) {
                lockContext.escalate(transaction);
            }
            if (lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.IX) {
                lockContext.promote(transaction, LockType.SIX);
            }
            if (lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.NL) {
                LockContext up = lockContext.parent;
                while (up != null) {

                    if (lockContext.lockman.getLockType(transaction, up.name) == LockType.SIX) {
                        return;
                    }
                    if (lockContext.lockman.getLockType(transaction, up.name) == LockType.S) {
                        return;
                    }
                    if (lockContext.lockman.getLockType(transaction, up.name) == LockType.X) {
                        return;
                    }
                    up = up.parent;
                }

                if (lockContext.parent != null) {
                    java.util.List<Long> names = new ArrayList();
                    names.add(lockContext.getResourceName().getNames().get(lockContext.getResourceName().getNames().size() - 1).getSecond());
                    up = lockContext.parent;
                    while (up.parent != null) {
                       /* if (lockContext.lockman.getLockType(transaction, up.name) == LockType.NL) {
                            lockContext.lockman.acquire(transaction, up.name, LockType.IS);
                        }*/
                       names.add(up.getResourceName().getNames().get(up.getResourceName().getNames().size() - 1).getSecond());
                       System.out.println(names.get(names.size() - 1));
                        up = up.parent;
                    }
                    while (!names.isEmpty()) {
                          if (lockContext.lockman.getLockType(transaction, up.name) == LockType.NL) {

                              up.acquire(transaction, LockType.IS);
                        }
                         // else if (lockContext.lockman.getLockType(transaction, up.name) == LockType.IX) {
                          //    up.promote(transaction, LockType.SIX);
                          //    return;
                          //}
                          up = up.childContext(names.get(names.size() - 1));
                          names.remove(names.size() - 1);
                    }
                }
                lockContext.acquire(transaction, LockType.S);
            }
            return;
        }

        if (lockType == LockType.X) {
            if (lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.X) {
                return;
            }

            LockContext up = lockContext.parent;
            while (up != null) {

                if (lockContext.lockman.getLockType(transaction, up.name) == LockType.X) {
                    return;
                }
                up = up.parent;
            }
            if (lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.IX) {
                lockContext.promote(transaction, LockType.X);
            }
            if (lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.SIX) {
                lockContext.promote(transaction, LockType.X);
            }

            if (lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.NL || lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.S || lockContext.lockman.getLockType(transaction, lockContext.name) == LockType.IS) {
                if (lockContext.parent != null) {
                    java.util.List<Long> names = new ArrayList();
                    names.add(lockContext.getResourceName().getNames().get(lockContext.getResourceName().getNames().size() - 1).getSecond());
                    up = lockContext.parent;
                    while (up.parent != null) {
                       /* if (lockContext.lockman.getLockType(transaction, up.name) == LockType.NL) {
                            lockContext.lockman.acquire(transaction, up.name, LockType.IS);
                        }*/
                        names.add(up.getResourceName().getNames().get(up.getResourceName().getNames().size() - 1).getSecond());
                        System.out.println(names.get(names.size() - 1));
                        up = up.parent;
                    }
                    while (!names.isEmpty()) {
                        if (lockContext.lockman.getLockType(transaction, up.name) == LockType.NL) {
                            up.acquire(transaction, LockType.IX);
                        } else  if (lockContext.lockman.getLockType(transaction, up.name) == LockType.IS || lockContext.lockman.getLockType(transaction, up.name) == LockType.S){
                            up.promote(transaction, LockType.IX);
                        }
                        up = up.childContext(names.get(names.size() - 1));
                        names.remove(names.size() - 1);
                    }
                }
                if (lockContext.lockman.getLockType(transaction, lockContext.name) != LockType.NL) {
                    lockContext.promote(transaction, LockType.X);
                } else {
                    lockContext.acquire(transaction, LockType.X);
                }
            }
        }

        if (lockType == LockType.NL) {
            return;
        }

    }

    // TODO(proj4_part2): add helper methods as you see fit
}
