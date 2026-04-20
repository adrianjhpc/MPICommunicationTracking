set(SOURCES 
    mpi_communication_tracking.c
)

# Create a shared library so it can be injected via LD_PRELOAD
add_library(mpi_comm_tracker SHARED ${SOURCES})

# Link the MPI C libraries to our target. 
target_link_libraries(mpi_comm_tracker PRIVATE MPI::MPI_C)

# Include the current directory to find mpi_communication_tracking.h
target_include_directories(mpi_comm_tracker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Add standard compiler flags for warnings and optimization
target_compile_options(mpi_comm_tracker PRIVATE -Wall -Wextra -O2)

# Ensure the library is built with Position Independent Code (PIC)
set_target_properties(mpi_comm_tracker PROPERTIES POSITION_INDEPENDENT_CODE ON)
