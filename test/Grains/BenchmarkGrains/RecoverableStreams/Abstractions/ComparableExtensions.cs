using System;

namespace Orleans.Streams
{
    /// <summary>
    /// Result codes for <see cref="ComparableExtensions.EasyCompareTo"/>.
    /// </summary>
    public enum EasyCompareToResult
    {
        /// <summary>
        /// The value precedes the value it was compared to.
        /// </summary>
        Before = -1,

        /// <summary>
        /// The value is equal to the value it was compared to.
        /// </summary>
        Equal = 0,

        /// <summary>
        /// The value follows the value it was compared to.
        /// </summary>
        After = 1,
    }

    /// <summary>
    /// Extension methods for <see cref="IComparable{T}"/>.
    /// </summary>
    public static class ComparableExtensions
    {
        /// <summary>
        /// Compares the current instance with another object of the same type and returns whether the current instance
        /// precedes, follows, or occurs in the same position in the sort order as the other object.
        /// </summary>
        /// <param name="value">The current instance.</param>
        /// <param name="otherValue">
        /// The other instance to compare with the current instance.
        /// </param>
        /// <returns>The result of the comparison of <paramref name="value"/> relative to <see cref="otherValue"/>.
        /// </returns>
        public static EasyCompareToResult EasyCompareTo(this IComparable value, object otherValue)
        {
            if (value == null) { throw new ArgumentNullException(nameof(value)); }
            if (otherValue == null) { throw new ArgumentNullException(nameof(otherValue)); }

            var compareToResult = value.CompareTo(otherValue);

            return ConvertCompareToIntToEasyCompareToResult(compareToResult);
        }

        /// <summary>
        /// Compares the current instance with another object of the same type and returns whether the current instance
        /// precedes, follows, or occurs in the same position in the sort order as the other object.
        /// </summary>
        /// <typeparam name="T">The type of the object to compare.</typeparam>
        /// <param name="value">The current instance.</param>
        /// <param name="otherValue">
        /// The other instance to compare with the current instance.
        /// </param>
        /// <returns>The result of the comparison of <paramref name="value"/> relative to <see cref="otherValue"/>.
        /// </returns>
        public static EasyCompareToResult EasyCompareTo<T>(this IComparable<T> value, T otherValue)
        {
            if (value == null) { throw new ArgumentNullException(nameof(value)); }
            if (otherValue == null) { throw new ArgumentNullException(nameof(otherValue)); }

            var compareToResult = value.CompareTo(otherValue);

            return ConvertCompareToIntToEasyCompareToResult(compareToResult);
        }

        /// <summary>
        /// Compares the current instance with another object of the same type and returns the instance that would come
        /// first if the instances were sorted according to <see cref="IComparable{T}.CompareTo"/>.
        /// </summary>
        /// <typeparam name="T">The type of the object to compare.</typeparam>
        /// <param name="value">The current instance.</param>
        /// <param name="otherValue">
        /// The other instance to compare with the current instance.
        /// </param>
        /// <returns>
        /// The instance that would come first. If the instances are equal, <paramref name="value"/> is returned.
        /// </returns>
        public static T ChooseEarlier<T>(this T value, T otherValue) where T : IComparable<T>
        {
            if (value == null) { throw new ArgumentNullException(nameof(value)); }
            if (otherValue == null) { throw new ArgumentNullException(nameof(otherValue)); }

            var compareToResult = value.CompareTo(otherValue);

            if (compareToResult <= 0)
            {
                return value;
            }

            return otherValue;
        }

        /// <summary>
        /// Compares the current instance with another object of the same type and returns the instance that would come
        /// last if the instances were sorted according to <see cref="IComparable{T}.CompareTo"/>.
        /// </summary>
        /// <typeparam name="T">The type of the object to compare.</typeparam>
        /// <param name="value">The current instance.</param>
        /// <param name="otherValue">
        /// The other instance to compare with the current instance.
        /// </param>
        /// <returns>
        /// The instance that would come last. If the instances are equal, <paramref name="value"/> is returned.
        /// </returns>
        public static T ChooseLater<T>(this T value, T otherValue) where T : IComparable<T>
        {
            if (value == null) { throw new ArgumentNullException(nameof(value)); }
            if (otherValue == null) { throw new ArgumentNullException(nameof(otherValue)); }

            var compareToResult = value.CompareTo(otherValue);

            if (compareToResult >= 0)
            {
                return value;
            }

            return otherValue;
        }

        /// <summary>
        /// Compares to a value returned by <see cref="IComparable.CompareTo"/> to a <see cref="EasyCompareToResult"/>.
        /// </summary>
        /// <param name="compareToResult">The value returned by <see cref="IComparable.CompareTo"/>.</param>
        /// <returns>The <see cref="EasyCompareToResult"/>.</returns>
        private static EasyCompareToResult ConvertCompareToIntToEasyCompareToResult(int compareToResult)
        {
            if (compareToResult < 0)
            {
                return EasyCompareToResult.Before;
            }
            else if (compareToResult == 0)
            {
                return EasyCompareToResult.Equal;
            }
            else
            {
                return EasyCompareToResult.After;
            }
        }
    }
}