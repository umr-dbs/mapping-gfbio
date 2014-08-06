#ifndef POINTVISUALIZATION_DIMENSION_H_
#define POINTVISUALIZATION_DIMENSION_H_

namespace pv {

	/**
	 * A dimension with width and height.
	 */
	class Dimension {
		public:
			/**
			 * Constructs a dimension given width and height.
			 * @param width
			 * @param height
			 */
			Dimension(double width, double height);

			/**
			 * @return width
			 */
			double getWidth() const;
			/**
			 * @return height
			 */
			double getHeight() const;

			/**
			 * Constructs a new Dimension with half width and height.
			 * @return half-dimension
			 */
			Dimension halve() const;
		private:
			const double width;
			const double height;
	};

}

#endif /* POINTVISUALIZATION_DIMENSION_H_ */
